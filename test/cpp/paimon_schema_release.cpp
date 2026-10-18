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
#include <cstddef>
#include <type_traits>

namespace {
enum class Fault { NONE, BEFORE_CONVERSION, PARTIAL_CONVERSION, AFTER_CONVERSION };
thread_local Fault injected_fault = Fault::NONE;
thread_local int acquired = 0;
thread_local int released = 0;
thread_local int deleted = 0;
thread_local size_t converted_columns = 0;
thread_local const void *exported_shell = nullptr;
thread_local void (*producer_release)(ArrowSchema *) = nullptr;
constexpr const char *failure = "injected Paimon schema conversion failure";

void TrackRelease(ArrowSchema *schema) {
	released++;
	schema->release = producer_release;
	producer_release(schema);
}

struct ScopedFailure {
	explicit ScopedFailure(Fault fault) {
		injected_fault = fault;
	}
	~ScopedFailure() {
		injected_fault = Fault::NONE;
	}
};

struct ScopedFormat {
	explicit ScopedFormat(ArrowSchema &schema) : schema(schema), format(schema.format) {
		schema.format = "invalid_schema_release_probe";
	}
	~ScopedFormat() {
		schema.format = format;
	}
	ArrowSchema &schema;
	const char *format;
};
} // namespace

extern "C" void RealDelete(void *) noexcept asm("__real__ZdlPv");
extern "C" void WrappedDelete(void *) noexcept asm("__wrap__ZdlPv");
extern "C" void RealSizedDelete(void *, std::size_t) noexcept asm("__real__ZdlPvm");
extern "C" void WrappedSizedDelete(void *, std::size_t) noexcept asm("__wrap__ZdlPvm");

static void TrackDelete(void *pointer) noexcept {
	if (pointer && pointer == exported_shell) {
		deleted++;
		exported_shell = nullptr;
	}
}

extern "C" void WrappedDelete(void *pointer) noexcept {
	TrackDelete(pointer);
	RealDelete(pointer);
}

extern "C" void WrappedSizedDelete(void *pointer, std::size_t bytes) noexcept {
	TrackDelete(pointer);
	RealSizedDelete(pointer, bytes);
}

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
	exported_shell = &schema;
	producer_release = schema.release;
	const_cast<ArrowSchema &>(schema).release = TrackRelease;
	if (injected_fault == Fault::BEFORE_CONVERSION) {
		throw duckdb::InvalidInputException(failure);
	}
	if (injected_fault == Fault::PARTIAL_CONVERSION) {
		if (schema.n_children < 2) {
			throw duckdb::InvalidInputException("fault probe requires two schema children");
		}
		ScopedFormat invalid(*schema.children[1]);
		try {
			RealPopulate(context, table, schema);
		} catch (...) {
			converted_columns = table.GetTypes().size();
			throw;
		}
		throw duckdb::InvalidInputException("fault probe did not trigger converter failure");
	}
	RealPopulate(context, table, schema);
	if (injected_fault == Fault::AFTER_CONVERSION) {
		converted_columns = table.GetTypes().size();
		throw duckdb::InvalidInputException(failure);
	}
}

TEST_CASE("Paimon binding releases exported schema on success and conversion failure", "[paimon][schema_release]") {
	const auto catalog = GENERATE(false, true);
	const auto fault = GENERATE(Fault::BEFORE_CONVERSION, Fault::PARTIAL_CONVERSION, Fault::AFTER_CONVERSION);
	CAPTURE(catalog, static_cast<int>(fault));
	acquired = released = deleted = 0;
	converted_columns = 0;
	duckdb::DBConfig config;
	config.options.load_extensions = false;
	duckdb::DuckDB db(nullptr, &config);
	db.LoadStaticExtension<duckdb::PaimonExtension>();
	duckdb::Connection con(db);
	const auto source = duckdb::Value(std::string(PAIMON_FIXTURE_DIR) + "/testdb.db/testtbl").ToSQLString();
	const auto attach = "ATTACH " + duckdb::Value(PAIMON_FIXTURE_DIR).ToSQLString() + " AS pm (TYPE paimon)";
	if (catalog) {
		REQUIRE_FALSE(con.Query(attach)->HasError());
	}
	const auto from = catalog ? std::string("pm.testdb.testtbl") : "paimon_scan(" + source + ")";

	auto baseline = con.Query("SELECT count(*) FROM " + from);
	REQUIRE_FALSE(baseline->HasError());
	auto rows = baseline->Fetch();
	REQUIRE(rows);
	REQUIRE(rows->size() == 1);
	REQUIRE(rows->GetValue(0, 0).GetValue<int64_t>() == 9);
	REQUIRE(acquired == (catalog ? 2 : 1));
	REQUIRE(released == acquired);
	REQUIRE(deleted == acquired);
	if (catalog) {
		REQUIRE_FALSE(con.Query("DETACH pm")->HasError());
		REQUIRE_FALSE(con.Query(attach)->HasError());
	}

	acquired = released = deleted = 0;
	std::string observed;
	{
		ScopedFailure injection(fault);
		try {
			auto result = con.Query("SELECT f1 FROM " + from);
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
	const auto expected = fault == Fault::PARTIAL_CONVERSION ? "Unsupported Internal Arrow Type" : failure;
	REQUIRE(observed.find(expected) != std::string::npos);
	if (fault == Fault::PARTIAL_CONVERSION) {
		REQUIRE(converted_columns == 1);
	} else if (fault == Fault::AFTER_CONVERSION) {
		REQUIRE(converted_columns >= 2);
	}
	REQUIRE(acquired == 1);
	REQUIRE(released == 1);
	REQUIRE(deleted == 1);

	auto alive = con.Query("SELECT 42");
	REQUIRE_FALSE(alive->HasError());
	auto row = alive->Fetch();
	REQUIRE(row);
	REQUIRE(row->size() == 1);
	REQUIRE(row->GetValue(0, 0).GetValue<int32_t>() == 42);
}
