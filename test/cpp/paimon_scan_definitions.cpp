/*-------------------------------------------------------------------------
 *
 * paimon_snapshot_binding.cpp
 *
 * Copyright (c) 2026, Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * IDENTIFICATION
 *    test/cpp/paimon_snapshot_binding.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "paimon_scan_definitions.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "catch.hpp"

#include <dlfcn.h>
#include <memory>
#include <tuple>

using namespace duckdb;

static void SQL(Connection &connection, const string &sql) {
	auto result = connection.Query(sql);
	INFO((result->HasError() ? result->GetError() : ""));
	REQUIRE_FALSE(result->HasError());
}

TEST_CASE("Paimon scan definitions match normal dynamic registration", "[paimon][scan_definitions]") {
	DBConfig config;
	config.SetOptionByName("allow_unsigned_extensions", Value::BOOLEAN(true));
	DuckDB db(nullptr, &config);
	Connection connection(db);
	SQL(connection, "LOAD " + Value(PAIMON_TEST_LOADABLE_PATH).ToSQLString());
	std::unique_ptr<void, decltype(&dlclose)> module(dlopen(PAIMON_TEST_LOADABLE_PATH, RTLD_NOW | RTLD_NOLOAD),
	                                                 dlclose);
	REQUIRE(module);
	auto visit = reinterpret_cast<decltype(&paimon_visit_scan_definitions_v1)>(
	    dlsym(module.get(), "paimon_visit_scan_definitions_v1"));
	REQUIRE(visit);
	Dl_info init_origin {}, visit_origin {};
	auto init = dlsym(module.get(), "paimon_duckdb_cpp_init");
	REQUIRE(init);
	REQUIRE(dladdr(init, &init_origin));
	REQUIRE(dladdr(reinterpret_cast<const void *>(visit), &visit_origin));
	REQUIRE(init_origin.dli_fbase == visit_origin.dli_fbase);
	REQUIRE(visit(nullptr, nullptr) == PaimonScanDefinitionStatusV1::INVALID_ARGUMENT);

	ExtensionLoader loader(*db.instance, "paimon");
	auto &functions = loader.GetTableFunction("paimon_scan").functions;
	REQUIRE(functions.Size() == 2);
	auto &context = *connection.context;
	auto path = functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});
	auto three =
	    functions.GetFunctionByArguments(context, {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR});
	Dl_info callback_origin {};
	REQUIRE(dladdr(reinterpret_cast<const void *>(path->get_bind_info), &callback_origin));
	REQUIRE(callback_origin.dli_fbase == visit_origin.dli_fbase);
	SQL(connection, "ATTACH " + Value(PAIMON_FIXTURE_DIR).ToSQLString() + " AS pm (TYPE paimon)");
	connection.BeginTransaction();
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName("pm", "testdb", "testtbl"));
	auto &table = Catalog::GetEntry(context, lookup).Cast<TableCatalogEntry>();
	unique_ptr<FunctionData> bind_data;
	auto attached = table.GetScanFunction(context, bind_data);
	REQUIRE(bind_data);
	connection.Rollback();

	auto identical = [](const TableFunction &a, const TableFunction &b) {
		return a.GetSignature() == b.GetSignature() &&
		       std::tie(a.function, a.bind, a.init_global, a.init_local, a.get_bind_info, a.pushdown_complex_filter,
		                a.get_partition_stats, a.to_string, a.projection_pushdown, a.function_info) ==
		           std::tie(b.function, b.bind, b.init_global, b.init_local, b.get_bind_info, b.pushdown_complex_filter,
		                    b.get_partition_stats, b.to_string, b.projection_pushdown, b.function_info);
	};
	REQUIRE(identical(attached, *three));

	struct State {
		const TableFunction &path;
		const TableFunction &three;
		decltype(identical) &matches;
		idx_t seen = 0;
		idx_t reject_after = 0;
		bool path_seen = false;
		bool three_seen = false;
	};
	auto visitor = [](void *data, const TableFunction *function) noexcept {
		auto &state = *static_cast<State *>(data);
		try {
			++state.seen;
			if (state.matches(*function, state.path) && !state.path_seen) {
				state.path_seen = true;
			} else if (state.matches(*function, state.three) && !state.three_seen) {
				state.three_seen = true;
			} else {
				return false;
			}
			return state.seen != state.reject_after;
		} catch (...) {
			return false;
		}
	};
	State state {*path, *three, identical};
	REQUIRE(visit(&state, visitor) == PaimonScanDefinitionStatusV1::OK);
	REQUIRE(state.seen == 2);
	REQUIRE(state.path_seen);
	REQUIRE(state.three_seen);

	for (idx_t stop : {1, 2}) {
		State rejected {*path, *three, identical};
		rejected.reject_after = stop;
		REQUIRE(visit(&rejected, visitor) == PaimonScanDefinitionStatusV1::VISITOR_REJECTED);
		REQUIRE(rejected.seen == stop);
	}
	SQL(connection, "SELECT count(*) FROM paimon_scan(" +
	                    Value(string(PAIMON_FIXTURE_DIR) + "/testdb.db/testtbl").ToSQLString() + ")");
}
