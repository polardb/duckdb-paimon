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

#include "paimon_extension.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/config.hpp"
#include "catch.hpp"

#include <string>
#include <type_traits>

namespace {
bool inject_failure = false;
int acquired = 0;
int released = 0;
void (*producer_release)(ArrowSchema *) = nullptr;
constexpr const char *failure = "injected Paimon schema conversion failure";

void TrackRelease(ArrowSchema *schema) {
	released++;
	schema->release = producer_release;
	producer_release(schema);
}

struct ScopedFailure {
	ScopedFailure() {
		inject_failure = true;
	}
	~ScopedFailure() {
		inject_failure = false;
	}
};
} // namespace

// GNU --wrap intercepts the real bind callback's external conversion call, not
// a copied binder.
extern "C" void RealPopulate(duckdb::ClientContext &, duckdb::ArrowTableSchema &,
                             const ArrowSchema &) asm("__real_" PAIMON_SCHEMA_POPULATE_SYMBOL);
extern "C" void WrappedPopulate(duckdb::ClientContext &, duckdb::ArrowTableSchema &,
                                const ArrowSchema &) asm("__wrap_" PAIMON_SCHEMA_POPULATE_SYMBOL);

static_assert(
    std::is_same<decltype(&WrappedPopulate), decltype(&duckdb::ArrowTableFunction::PopulateArrowTableSchema)>::value,
    "update the schema fault probe for the pinned DuckDB signature");

extern "C" void WrappedPopulate(duckdb::ClientContext &context, duckdb::ArrowTableSchema &table,
                                const ArrowSchema &schema) {
	if (!schema.release) {
		throw duckdb::InvalidInputException("fault probe received a released schema");
	}
	acquired++;
	producer_release = schema.release;
	const_cast<ArrowSchema &>(schema).release = TrackRelease;
	if (inject_failure) {
		throw duckdb::InvalidInputException(failure);
	}
	RealPopulate(context, table, schema);
}

TEST_CASE("Paimon binding releases exported schema on success and conversion failure", "[paimon][schema_release]") {
	duckdb::DBConfig config;
	config.options.load_extensions = false;
	duckdb::DuckDB db(nullptr, &config);
	db.LoadStaticExtension<duckdb::PaimonExtension>();
	duckdb::Connection con(db);
	const auto source = duckdb::Value(std::string(PAIMON_FIXTURE_DIR) + "/testdb.db/testtbl").ToSQLString();

	auto baseline = con.Query("SELECT count(*) FROM paimon_scan(" + source + ")");
	REQUIRE_FALSE(baseline->HasError());
	auto rows = baseline->Fetch();
	REQUIRE(rows);
	REQUIRE(rows->size() == 1);
	REQUIRE(rows->GetValue(0, 0).GetValue<int64_t>() == 9);
	REQUIRE(acquired == 1);
	REQUIRE(released == 1);

	acquired = released = 0;
	std::string observed;
	{
		ScopedFailure fault;
		try {
			auto result = con.Query("SELECT f1 FROM paimon_scan(" + source + ")");
			if (!result->HasError()) {
				// Fetch also drives a lazy QueryResult on the maintained v2 host.
				(void)result->Fetch();
			}
			if (result->HasError()) {
				observed = result->GetError();
			}
		} catch (const std::exception &error) {
			observed = error.what();
		}
	}
	INFO(observed);
	REQUIRE(observed.find(failure) != std::string::npos);
	REQUIRE(acquired == 1);
	REQUIRE(released == 1);

	auto alive = con.Query("SELECT 42");
	REQUIRE_FALSE(alive->HasError());
	auto row = alive->Fetch();
	REQUIRE(row);
	REQUIRE(row->size() == 1);
	REQUIRE(row->GetValue(0, 0).GetValue<int32_t>() == 42);
}
