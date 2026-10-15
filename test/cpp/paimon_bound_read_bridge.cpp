/*
 * Copyright (c) 2026, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon_read_bridge.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "catch.hpp"
#include "yyjson.hpp"
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <type_traits>
#if defined(__unix__) || defined(__APPLE__)
#include <dlfcn.h>
#endif

using namespace duckdb;

static_assert(std::is_standard_layout<PaimonReadBridgeV1>::value, "Bridge must have C layout");
static_assert(std::is_standard_layout<PaimonBoundReadViewV1>::value, "Views must have C layout");

static string Text(PaimonReadString value) {
	return string(value.data, value.size);
}

static void CheckSchema(const PaimonBoundReadViewV1 &view, const string &path) {
	using namespace duckdb_yyjson;
	std::ifstream input(path);
	REQUIRE(input.good());
	string expected((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
	std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> actual(
	    yyjson_read(view.schema_json.data, view.schema_json.size, 0), yyjson_doc_free);
	std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> oracle(yyjson_read(expected.data(), expected.size(), 0),
	                                                               yyjson_doc_free);
	REQUIRE(actual);
	REQUIRE(oracle);
	auto actual_root = yyjson_doc_get_root(actual.get());
	auto expected_root = yyjson_doc_get_root(oracle.get());
	REQUIRE(yyjson_is_obj(actual_root));
	REQUIRE(yyjson_is_obj(expected_root));
	size_t index, count;
	yyjson_val *key, *value;
	size_t expected_count = yyjson_obj_size(expected_root);
	yyjson_obj_foreach(expected_root, index, count, key, value) {
		auto name = yyjson_get_str(key);
		auto found = yyjson_obj_get(actual_root, name);
		// Native TableSchema omits an absent optional comment when serializing.
		if (string(name) == "comment" && yyjson_is_null(value) && !found) {
			expected_count--;
			continue;
		}
		INFO(name);
		REQUIRE(yyjson_equals(found, value));
	}
	REQUIRE(yyjson_obj_size(actual_root) == expected_count);
}

static void SQL(Connection &con, const string &query) {
	auto result = con.Query(query);
	INFO((result->HasError() ? result->GetError() : ""));
	REQUIRE_FALSE(result->HasError());
}

struct Descriptor {
	const PaimonReadBridgeV1 &api;
	void *handle = nullptr;
	explicit Descriptor(const PaimonReadBridgeV1 &api_p) : api(api_p) {
	}
	Descriptor(const Descriptor &) = delete;
	Descriptor &operator=(const Descriptor &) = delete;
	~Descriptor() {
		api.release(handle);
	}
	PaimonBoundReadViewV1 View() const {
		PaimonBoundReadViewV1 result {};
		REQUIRE(api.describe(handle, &result, sizeof(result)) == PAIMON_READ_OK);
		return result;
	}
};

struct BridgeBinding {
	TableFunction function;
	unique_ptr<FunctionData> data;
};

struct BridgeFixture {
	DBConfig config;
	unique_ptr<DuckDB> db;
	unique_ptr<Connection> reader;
	unique_ptr<Connection> writer;
	string warehouse;

	BridgeFixture() {
		config.SetOptionByName("allow_unsigned_extensions", Value(true));
		db = make_uniq<DuckDB>(nullptr, &config);
		reader = make_uniq<Connection>(*db);
		writer = make_uniq<Connection>(*db);
#if !PAIMON_BRIDGE_BUILTIN
		SQL(*reader, "LOAD " + Value(PAIMON_BRIDGE_EXTENSION_PATH).ToSQLString());
#endif
		static std::atomic<uint64_t> next_id {0};
		auto *root = std::getenv("PAIMON_TEST_ROOT");
		auto base = root ? std::filesystem::path(root) : std::filesystem::temp_directory_path();
		std::filesystem::create_directories(base);
		auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
		warehouse = (base / ("paimon-bridge-" + std::to_string(stamp) + "-" + std::to_string(next_id++))).string();
		REQUIRE(std::filesystem::create_directory(warehouse));
		SQL(*reader, "ATTACH " + Value(warehouse).ToSQLString() + " AS pm (TYPE paimon)");
		SQL(*writer, "CREATE SCHEMA pm.race");
		SQL(*writer, "CREATE TABLE pm.race.t (id INTEGER, value INTEGER)");
	}

	BridgeBinding Bind(int entry = 0, const string &table = "t") {
		reader->BeginTransaction();
		try {
			BridgeBinding bound;
			auto &context = *reader->context;
			if (entry == 2) {
				auto &catalog_entry = Catalog::GetEntry<TableCatalogEntry>(context, "pm", "race", table);
				bound.function = catalog_entry.GetScanFunction(context, bound.data);
			} else {
				auto &catalog_entry = Catalog::GetSystemCatalog(context).GetEntry<TableFunctionCatalogEntry>(
				    context, DEFAULT_SCHEMA, "paimon_scan");
				auto argument_count = entry == 0 ? 1 : 3;
				for (idx_t i = 0; i < catalog_entry.functions.Size(); i++) {
					auto function = catalog_entry.functions.GetFunctionByOffset(i);
					if (function.arguments.size() == idx_t(argument_count)) {
						bound.function = function;
					}
				}
				REQUIRE(bound.function.bind);
				vector<Value> args = entry == 0 ? vector<Value> {Value(warehouse + "/race.db/" + table)}
				                                : vector<Value> {Value(warehouse), Value("race"), Value(table)};
				named_parameter_map_t parameters;
				vector<LogicalType> input_types, return_types;
				vector<string> input_names, names;
				TableFunctionRef ref;
				TableFunctionBindInput input(args, parameters, input_types, input_names, nullptr, nullptr,
				                             bound.function, ref);
				bound.data = bound.function.bind(context, input, return_types, names);
			}
			reader->Commit();
			return bound;
		} catch (...) {
			reader->Rollback();
			throw;
		}
	}

	const PaimonReadBridgeV1 &API(const BridgeBinding &bound) {
		// Fixture owns LOAD and the unmodified catalog. Production consumers must
		// establish provider/callback identity before interpreting function_info.
		auto *info = dynamic_cast<const PaimonReadBridgeInfo *>(bound.function.function_info.get());
		REQUIRE(info);
		REQUIRE(info->api.magic == PAIMON_READ_BRIDGE_MAGIC);
		REQUIRE(info->api.version == PAIMON_READ_BRIDGE_VERSION);
		REQUIRE(info->api.struct_size == sizeof(PaimonReadBridgeV1));
#if !PAIMON_BRIDGE_BUILTIN && (defined(__unix__) || defined(__APPLE__))
		Dl_info origin {};
		REQUIRE(dladdr(reinterpret_cast<const void *>(info->api.retain), &origin) != 0);
		REQUIRE(origin.dli_fname);
		REQUIRE(std::filesystem::equivalent(origin.dli_fname, PAIMON_BRIDGE_EXTENSION_PATH));
#endif
		return info->api;
	}

	void Retain(const BridgeBinding &bound, Descriptor &descriptor) {
		char error[128];
		auto status = descriptor.api.retain(bound.data.get(), &descriptor.handle, error, sizeof(error));
		INFO(error);
		REQUIRE(status == PAIMON_READ_OK);
		REQUIRE(error[0] == '\0');
		REQUIRE(descriptor.handle);
	}
};

TEST_CASE_METHOD(BridgeFixture, "Paimon bridge: retained identity outlives binding", "[paimon][bound_read_bridge]") {
	auto entry = GENERATE(0, 1, 2);
	SQL(*writer, "INSERT INTO pm.race.t VALUES (1, 10)");
	auto bound = Bind(entry);
	auto &api = API(bound);
	REQUIRE(api.accepts_bind(bound.data.get()) == 1);
	Descriptor old(api);
	Retain(bound, old);
	auto old_schema = Text(old.View().schema_json);
	bound.data.reset();
	SQL(*writer, "INSERT INTO pm.race.t VALUES (2, 20)");
	auto fresh_bound = Bind(entry);
	REQUIRE(bound.function.function_info == fresh_bound.function.function_info);
	Descriptor fresh(api);
	Retain(fresh_bound, fresh);
	auto old_view = old.View();
	REQUIRE(old_view.has_snapshot == 1);
	REQUIRE(old_view.snapshot_id == 1);
	REQUIRE(fresh.View().snapshot_id == 2);
	REQUIRE(Text(old_view.schema_json) == old_schema);
	REQUIRE(Text(old_view.location) == warehouse + "/race.db/t");
	REQUIRE(Text(old_view.branch) == "main");
	REQUIRE(Text(old_view.database) == "race");
	bool pinned_option = false;
	for (uint64_t i = 0; i < old_view.option_count; i++) {
		if (Text(old_view.options[i].key) == "scan.snapshot-id") {
			REQUIRE(Text(old_view.options[i].value) == "1");
			pinned_option = true;
		}
	}
	REQUIRE(pinned_option);
}

#if !PAIMON_BRIDGE_BUILTIN
TEST_CASE("Paimon bridge: missing dynamic producer cannot become success", "[paimon][bound_read_bridge_dynamic]") {
	DBConfig config;
	config.SetOptionByName("allow_unsigned_extensions", Value(true));
	DuckDB db(nullptr, &config);
	Connection con(db);
	auto catalog_absent = [&]() {
		auto result = con.Query("SELECT count(*) FROM duckdb_functions() WHERE function_name='paimon_scan'");
		REQUIRE_FALSE(result->HasError());
		REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 0);
	};
	catalog_absent();
	auto missing = std::filesystem::path(PAIMON_BRIDGE_EXTENSION_PATH).parent_path() /
	               ("missing-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())) /
	               "paimon.duckdb_extension";
	REQUIRE_FALSE(std::filesystem::exists(missing));
	auto result = con.Query("LOAD " + Value(missing.string()).ToSQLString());
	REQUIRE(result->HasError());
	REQUIRE(result->GetError().find("not found") != string::npos);
	catalog_absent();
}
#endif

TEST_CASE_METHOD(BridgeFixture, "Paimon bridge: empty is not latest", "[paimon][bound_read_bridge]") {
	auto bound = Bind(GENERATE(0, 1, 2));
	auto &api = API(bound);
	Descriptor empty(api);
	Retain(bound, empty);
	SQL(*writer, "INSERT INTO pm.race.t VALUES (1, 10)");
	REQUIRE(empty.View().has_snapshot == 0);
	REQUIRE(empty.View().snapshot_id == 0);
}

TEST_CASE_METHOD(BridgeFixture, "Paimon bridge: retained schema identity", "[paimon][bound_read_bridge]") {
	auto entry = GENERATE(0, 1, 2);
	auto path = warehouse + "/race.db/evolution";
	std::filesystem::copy(string(PAIMON_FIXTURE_DIR) + "/schema_evolution.db/mixed_types", path,
	                      std::filesystem::copy_options::recursive);
	auto published = path + "/schema/schema-1";
	auto staged = warehouse + "/staged-schema-1";
	std::filesystem::rename(published, staged);
	auto bound = Bind(entry, "evolution");
	auto &api = API(bound);
	Descriptor old(api);
	Retain(bound, old);
	auto schema = Text(old.View().schema_json);
	REQUIRE(old.View().schema_id == 0);
	CheckSchema(old.View(), path + "/schema/schema-0");
	map<string, string> options;
	auto view = old.View();
	for (uint64_t i = 0; i < view.option_count; i++) {
		REQUIRE(options.emplace(Text(view.options[i].key), Text(view.options[i].value)).second);
	}
	const map<string, string> expected_options {{"bucket", "-1"},
	                                            {"file.format", "parquet"},
	                                            {"branch", "main"},
	                                            {"file-system", "local"},
	                                            {"read.batch-size", std::to_string(STANDARD_VECTOR_SIZE)},
	                                            {"scan.mode", "from-snapshot-full"},
	                                            {"scan.snapshot-id", "1"}};
	REQUIRE(options == expected_options);
	bound.data.reset();
	std::filesystem::rename(staged, published);
	SQL(*reader, "DETACH pm");
	SQL(*reader, "ATTACH " + Value(warehouse).ToSQLString() + " AS pm (TYPE paimon)");
	auto fresh_bound = Bind(entry, "evolution");
	Descriptor fresh(api);
	Retain(fresh_bound, fresh);
	REQUIRE(fresh.View().schema_id == 1);
	CheckSchema(fresh.View(), published);
	REQUIRE(Text(fresh.View().schema_json) != schema);
	REQUIRE(Text(old.View().schema_json) == schema);
}

TEST_CASE_METHOD(BridgeFixture, "Paimon bridge: explicit branch is preserved", "[paimon][bound_read_bridge]") {
	SQL(*writer, "INSERT INTO pm.race.t VALUES (1, 10)");
	auto path = warehouse + "/race.db/t";
	auto branch = path + "/branch/branch-dev";
	std::filesystem::create_directories(branch);
	std::filesystem::copy(path + "/schema", branch + "/schema", std::filesystem::copy_options::recursive);
	std::filesystem::copy(path + "/snapshot", branch + "/snapshot", std::filesystem::copy_options::recursive);
	auto bound = Bind(GENERATE(0, 1, 2), "t$branch_dev");
	auto &api = API(bound);
	Descriptor descriptor(api);
	Retain(bound, descriptor);
	SQL(*writer, "INSERT INTO pm.race.t VALUES (2, 20)");
	REQUIRE(Text(descriptor.View().branch) == "dev");
	REQUIRE(descriptor.View().snapshot_id == 1);
}

TEST_CASE_METHOD(BridgeFixture, "Paimon bridge: singleton across databases and overloads",
                 "[paimon][bound_read_bridge]") {
	auto path = Bind(0);
	auto three = Bind(1);
	auto catalog = Bind(2);
	BridgeFixture other;
	auto independent = other.Bind();
	REQUIRE(path.function.function_info == three.function.function_info);
	REQUIRE(path.function.function_info == catalog.function.function_info);
	REQUIRE(path.function.function_info == independent.function.function_info);
}

TEST_CASE_METHOD(BridgeFixture, "Paimon bridge: descriptor does not access manifests", "[paimon][bound_read_bridge]") {
	SQL(*writer, "INSERT INTO pm.race.t VALUES (1, 10)");
	auto bound = Bind();
	std::filesystem::remove_all(warehouse + "/race.db/t");
	auto &api = API(bound);
	Descriptor descriptor(api);
	Retain(bound, descriptor);
	REQUIRE(descriptor.View().snapshot_id == 1);
	REQUIRE_FALSE(Text(descriptor.View().schema_json).empty());
}

TEST_CASE_METHOD(BridgeFixture, "Paimon bridge: invalid inputs fail closed", "[paimon][bound_read_bridge]") {
	auto bound = Bind();
	auto &api = API(bound);
	REQUIRE(api.accepts_bind(nullptr) == 0);
	TableFunctionData foreign;
	REQUIRE(api.accepts_bind(&foreign) == 0);
	char error[4] {'x', 'x', 'x', 'x'};
	void *handle = &foreign;
	REQUIRE(api.retain(&foreign, &handle, error, sizeof(error)) == PAIMON_READ_INVALID_ARGUMENT);
	REQUIRE(handle == nullptr);
	REQUIRE(error[3] == '\0');
	REQUIRE(api.retain(nullptr, &handle, nullptr, 0) == PAIMON_READ_INVALID_ARGUMENT);
	REQUIRE(api.retain(bound.data.get(), nullptr, nullptr, 0) == PAIMON_READ_INVALID_ARGUMENT);
	Descriptor descriptor(api);
	Retain(bound, descriptor);
	PaimonBoundReadViewV1 view {};
	view.has_snapshot = 42;
	REQUIRE(api.describe(descriptor.handle, &view, sizeof(view) - 1) == PAIMON_READ_INVALID_ARGUMENT);
	REQUIRE(view.has_snapshot == 42);
	REQUIRE(api.describe(descriptor.handle, nullptr, sizeof(view)) == PAIMON_READ_INVALID_ARGUMENT);
	REQUIRE(api.describe(nullptr, &view, sizeof(view)) == PAIMON_READ_INVALID_ARGUMENT);
	REQUIRE(view.has_snapshot == 0);
	api.release(nullptr);
}

TEST_CASE_METHOD(BridgeFixture, "Paimon bridge: unsupported binding is not empty success",
                 "[paimon][bound_read_bridge]") {
	SQL(*writer, "CREATE TABLE pm.race.unsupported (id INTEGER PRIMARY KEY)");
	auto bound = Bind(GENERATE(0, 1, 2), "unsupported");
	auto &api = API(bound);
	Descriptor descriptor(api);
	char error[128];
	REQUIRE(api.retain(bound.data.get(), &descriptor.handle, error, sizeof(error)) == PAIMON_READ_UNSUPPORTED);
	REQUIRE(descriptor.handle == nullptr);
	REQUIRE(string(error) == "Bound Paimon read is unsupported");
}
