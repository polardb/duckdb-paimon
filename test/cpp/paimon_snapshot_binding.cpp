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

#include "paimon_extension.hpp"
#include "paimon_functions.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include "catch.hpp"
#include "yyjson.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>

using namespace duckdb;

static void SQL(Connection &connection, const string &sql) {
	auto result = connection.Query(sql);
	INFO((result->HasError() ? result->GetError() : ""));
	REQUIRE_FALSE(result->HasError());
}

enum class Entry { PATH, THREE_PART, CATALOG };

struct BoundScan {
	TableFunction function;
	unique_ptr<FunctionData> data;
	vector<LogicalType> types;
	vector<string> names;
};

// Retain the registered callback's binding while a separate writer commits.
// SQL PREPARE/EXECUTE may rebind and cannot establish this contract by itself.
static BoundScan Bind(Connection &reader, const string &warehouse, Entry entry, const string &table = "t",
                      named_parameter_map_t parameters = {}) {
	reader.BeginTransaction();
	try {
		BoundScan bound;
		auto &context = *reader.context;
		if (entry == Entry::CATALOG) {
			EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, table);
			auto &catalog_entry = Catalog::GetEntry(context, "pm", "race", lookup).Cast<TableCatalogEntry>();
			bound.types = catalog_entry.GetTypes();
			bound.names = catalog_entry.GetColumns().GetColumnNames();
			bound.function = catalog_entry.GetScanFunction(context, bound.data);
		} else {
			vector<Value> args = entry == Entry::PATH ? vector<Value> {Value(warehouse + "/race.db/" + table)}
			                                          : vector<Value> {Value(warehouse), Value("race"), Value(table)};
			vector<LogicalType> argument_types(args.size(), LogicalType::VARCHAR);
			auto info = PaimonFunctions::GetPaimonScanFunction();
			bound.function = info.functions.GetFunctionByArguments(context, argument_types);
			vector<LogicalType> input_types;
			vector<string> input_names;
			TableFunctionRef ref;
			TableFunctionBindInput input(args, parameters, input_types, input_names, nullptr, nullptr, bound.function,
			                             ref);
			bound.data = bound.function.bind(context, input, bound.types, bound.names);
		}
		INFO("all scan entrypoints must require rebinding");
		REQUIRE(!bound.data->SupportStatementCache());
		reader.Commit();
		return bound;
	} catch (...) {
		reader.Rollback();
		throw;
	}
}

static idx_t Count(Connection &reader, BoundScan &bound) {
	GetPartitionStatsInput input(bound.function, *bound.data);
	auto stats = bound.function.get_partition_stats(*reader.context, input);
	INFO("exact statistics missing");
	REQUIRE((stats.size() == 1 && stats[0].count_type == CountType::COUNT_EXACT));
	return stats[0].count;
}

using Rows = vector<vector<Value>>;

static Rows Read(Connection &reader, BoundScan &bound, bool partial = false) {
	vector<column_t> columns;
	for (idx_t i = 0; i < bound.types.size(); i++) {
		columns.push_back(i);
	}
	TableFunctionInitInput input(bound.data.get(), columns, {}, nullptr);
	auto global = bound.function.init_global(*reader.context, input);
	ThreadContext thread(*reader.context);
	ExecutionContext execution(*reader.context, thread, nullptr);
	auto local = bound.function.init_local(execution, input, global.get());
	TableFunctionInput scan_input(bound.data.get(), local.get(), global.get());
	DataChunk output;
	output.Initialize(Allocator::Get(*reader.context), bound.types);
	Rows rows;
	while (true) {
		output.Reset();
		bound.function.function(*reader.context, scan_input, output);
		if (output.size() == 0) {
			break;
		}
		for (idx_t i = 0; i < output.size(); i++) {
			vector<Value> row;
			for (idx_t c = 0; c < output.ColumnCount(); c++) {
				row.push_back(output.GetValue(c, i));
			}
			rows.push_back(std::move(row));
		}
		if (partial) {
			break;
		}
	}
	std::sort(rows.begin(), rows.end(),
	          [](const vector<Value> &a, const vector<Value> &b) { return a[0].ToString() < b[0].ToString(); });
	return rows;
}

static void ExpectRows(const Rows &actual, const Rows &expected) {
	INFO("unexpected row count: " + std::to_string(actual.size()));
	REQUIRE(actual.size() == expected.size());
	for (idx_t r = 0; r < expected.size(); r++) {
		INFO("unexpected column count");
		REQUIRE(actual[r].size() == expected[r].size());
		for (idx_t c = 0; c < expected[r].size(); c++) {
			INFO("unexpected value or SQL type");
			REQUIRE(
			    (actual[r][c].type() == expected[r][c].type() && Value::NotDistinctFrom(actual[r][c], expected[r][c])));
		}
	}
}

static void PushEqual(Connection &reader, BoundScan &bound, idx_t column, int value) {
	LogicalGet get(0, bound.function, nullptr, bound.types, bound.names);
	for (idx_t i = 0; i < bound.types.size(); i++) {
		get.AddColumnId(i);
	}
	vector<unique_ptr<Expression>> filters;
	filters.push_back(make_uniq<BoundComparisonExpression>(
	    ExpressionType::COMPARE_EQUAL,
	    make_uniq<BoundColumnRefExpression>(bound.types[column], ColumnBinding(0, column)),
	    make_uniq<BoundConstantExpression>(Value(value))));
	bound.function.pushdown_complex_filter(*reader.context, get, bound.data.get(), filters);
	INFO("DuckDB residual filter was removed");
	REQUIRE(filters.size() == 1);
}

static void CheckColumnSignatures() {
	auto signature = [](const string &fields, const string &keys = "[]", const string &partitions = "[]") {
		return PaimonFunctions::GetColumnSignature("{\"fields\":" + fields + ",\"partitionKeys\":" + partitions +
		                                           ",\"primaryKeys\":" + keys + "}");
	};
	const string fields = R"([{"id":0,"name":"v","type":"INT"}])";
	auto baseline = signature(fields);
	INFO("field identity was ignored");
	REQUIRE(baseline != signature(R"([{"id":1,"name":"v","type":"INT"}])"));
	INFO("nullability was ignored");
	REQUIRE(baseline != signature(R"([{"id":0,"name":"v","type":"INT NOT NULL"}])"));
	INFO("primary key was ignored");
	REQUIRE(baseline != signature(fields, "[\"v\"]"));
	INFO("partition key was ignored");
	REQUIRE(baseline != signature(fields, "[]", "[\"v\"]"));
	const string nested = R"([{"id":0,"name":"r","type":{"type":"ROW","fields":[{"id":1,"name":"v","type":"INT"}]}}])";
	const string annotated =
	    R"([{"id":0,"name":"r","type":{"type":"ROW","fields":[{"id":1,"name":"v","type":"INT","description":"new comment"}]}}])";
	const string changed = R"([{"id":0,"name":"r","type":{"type":"ROW","fields":[{"id":2,"name":"v","type":"INT"}]}}])";
	INFO("nested comments changed column identity");
	REQUIRE(signature(nested) == signature(annotated));
	INFO("nested field identity was ignored");
	REQUIRE(signature(nested) != signature(changed));
}

struct BindingFixture {
	DuckDB db;
	Connection reader;
	Connection writer;
	string warehouse;
	const string fixtures = PAIMON_FIXTURE_DIR;
	const string schema_fixtures = PAIMON_SCHEMA_FIXTURE_DIR;
	const string native_fixtures = PAIMON_NATIVE_FIXTURE_DIR;

	BindingFixture() : db(nullptr), reader(db), writer(db) {
		db.LoadStaticExtension<PaimonExtension>();
		static std::atomic<uint64_t> next_id {0};
		const auto *root = std::getenv("PAIMON_TEST_ROOT");
		auto base = root ? std::filesystem::path(root) : std::filesystem::temp_directory_path();
		std::filesystem::create_directories(base);
		const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
		warehouse = (base / ("paimon-binding-" + std::to_string(stamp) + "-" + std::to_string(next_id++))).string();
		REQUIRE(std::filesystem::create_directory(warehouse));
		INFO("Fixture: " << warehouse);
		SQL(reader, "ATTACH " + Value(warehouse).ToSQLString() + " AS pm (TYPE paimon)");
		SQL(writer, "CREATE SCHEMA pm.race");
		SQL(writer, "CREATE TABLE pm.race.t (id INTEGER, value INTEGER)");
	}
};

// Branches share data and manifests at the table root; only metadata is copied.
static string CopyBranch(const string &path, const string &branch = "dev") {
	const auto destination = path + "/branch/branch-" + branch;
	REQUIRE(std::filesystem::create_directories(destination));
	std::filesystem::copy(path + "/schema", destination + "/schema", std::filesystem::copy_options::recursive);
	if (std::filesystem::exists(path + "/snapshot")) {
		std::filesystem::copy(path + "/snapshot", destination + "/snapshot", std::filesystem::copy_options::recursive);
	}
	return destination;
}

static void SetSchemaOption(const string &path, const string &key, const string &value) {
	using namespace duckdb_yyjson;
	std::ifstream input(path);
	REQUIRE(input.good());
	const string json((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
	std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> source(yyjson_read(json.data(), json.size(), 0),
	                                                               yyjson_doc_free);
	REQUIRE(source);
	std::unique_ptr<yyjson_mut_doc, decltype(&yyjson_mut_doc_free)> doc(yyjson_doc_mut_copy(source.get(), nullptr),
	                                                                    yyjson_mut_doc_free);
	REQUIRE(doc);
	auto options = yyjson_mut_obj_get(yyjson_mut_doc_get_root(doc.get()), "options");
	REQUIRE(yyjson_mut_is_obj(options));
	REQUIRE(yyjson_mut_obj_put(options, yyjson_mut_strcpy(doc.get(), key.c_str()),
	                           yyjson_mut_strcpy(doc.get(), value.c_str())));
	size_t length;
	std::unique_ptr<char, decltype(&std::free)> output(yyjson_mut_write(doc.get(), 0, &length), std::free);
	REQUIRE(output);
	std::ofstream file(path, std::ios::trunc);
	file.write(output.get(), length);
	file.close();
	REQUIRE_FALSE(file.fail());
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: main branch options", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART, Entry::CATALOG);
	const auto options = GENERATE("", "'branch'='main'", "'branch'=''", "'branch'=' \t\r\n '",
	                              "'scan.fallback-branch'=''", "'scan.fallback-branch'=' \t\r\n '");
	CAPTURE(static_cast<int>(entry), options, warehouse);
	SQL(writer, "CREATE TABLE pm.race.main_options (id INTEGER, value INTEGER)" +
	                (string(options).empty() ? "" : " WITH (" + string(options) + ")"));
	SQL(writer, "INSERT INTO pm.race.main_options VALUES (1,10),(2,20)");
	for (const auto &table : {"main_options", "main_options$branch_main"}) {
		auto bound = Bind(reader, warehouse, entry, table);
		REQUIRE(Count(reader, bound) == 2);
		ExpectRows(Read(reader, bound), {{Value(1), Value(10)}, {Value(2), Value(20)}});
	}
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: branch snapshots", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART, Entry::CATALOG);
	const auto option_selected = GENERATE(false, true);
	CAPTURE(static_cast<int>(entry), option_selected, warehouse);
	SQL(writer, "INSERT INTO pm.race.t VALUES (1,10)");
	const auto path = warehouse + "/race.db/t";
	const auto branch = CopyBranch(path);
	SQL(writer, "INSERT INTO pm.race.t VALUES (2,20),(3,30)");
	if (option_selected) {
		SetSchemaOption(path + "/schema/schema-0", "branch", "dev");
	}
	const string table = option_selected ? "t" : "t$branch_dev";
	auto bound = Bind(reader, warehouse, entry, table);
	REQUIRE(Count(reader, bound) == 1);
	ExpectRows(Read(reader, bound), {{Value(1), Value(10)}});
	auto main = Bind(reader, warehouse, entry, "t$branch_main");
	REQUIRE(Count(reader, main) == 3);
	ExpectRows(Read(reader, main), {{Value(1), Value(10)}, {Value(2), Value(20)}, {Value(3), Value(30)}});

	// Publish a genuine later snapshot on dev without using a branch writer.
	std::filesystem::copy(path + "/snapshot", branch + "/snapshot",
	                      std::filesystem::copy_options::recursive | std::filesystem::copy_options::overwrite_existing);
	REQUIRE(Count(reader, bound) == 1);
	ExpectRows(Read(reader, bound), {{Value(1), Value(10)}});
	auto fresh = Bind(reader, warehouse, entry, table);
	REQUIRE(Count(reader, fresh) == 3);
	ExpectRows(Read(reader, fresh), {{Value(1), Value(10)}, {Value(2), Value(20)}, {Value(3), Value(30)}});

	// Main still has snapshot 1: expiry on dev must not fall back to it.
	REQUIRE(std::filesystem::remove(branch + "/snapshot/snapshot-1"));
	REQUIRE_THROWS_WITH(Count(reader, bound), Catch::Contains("snapshot"));
	REQUIRE_THROWS_WITH(Read(reader, bound), Catch::Contains("snapshot"));
	SQL(reader, "SELECT 42");
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: branch historical schema", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART, Entry::CATALOG);
	CAPTURE(static_cast<int>(entry), warehouse);
	const auto path = warehouse + "/race.db/branch_history";
	std::filesystem::copy(fixtures + "/schema_evolution.db/mixed_types", path,
	                      std::filesystem::copy_options::recursive);
	CopyBranch(path);
	// Data files use schema 0 while both branches bind schema 1. Only dev
	// retains the historical schema required to map the stored columns.
	std::filesystem::rename(path + "/schema/schema-0", warehouse + "/hidden-main-schema-0");
	auto dev = Bind(reader, warehouse, entry, "branch_history$branch_dev");
	REQUIRE(dev.names == vector<string> {"old", "id", "discard"});
	REQUIRE(Count(reader, dev) == 1);
	ExpectRows(Read(reader, dev), {{Value("before"), Value(1), Value::BIGINT(10)}});
	// Prove the fixture actually requires historical schema lookup. The latest
	// main schema is still valid, so binding succeeds but execution must fail.
	auto main = Bind(reader, warehouse, entry, "branch_history$branch_main");
	REQUIRE_THROWS_WITH(Read(reader, main), Catch::Contains("Not exist"));
	ExpectRows(Read(reader, dev), {{Value("before"), Value(1), Value::BIGINT(10)}});
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: empty branch", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART, Entry::CATALOG);
	CAPTURE(static_cast<int>(entry), warehouse);
	const auto path = warehouse + "/race.db/t";
	const auto branch = CopyBranch(path);
	SQL(writer, "INSERT INTO pm.race.t VALUES (1,10),(2,20)");
	auto empty = Bind(reader, warehouse, entry, "t$branch_dev");
	REQUIRE(Count(reader, empty) == 0);
	ExpectRows(Read(reader, empty), {});
	std::filesystem::copy(path + "/snapshot", branch + "/snapshot", std::filesystem::copy_options::recursive);
	REQUIRE(Count(reader, empty) == 0);
	ExpectRows(Read(reader, empty), {});
	auto fresh = Bind(reader, warehouse, entry, "t$branch_dev");
	REQUIRE(Count(reader, fresh) == 2);
	ExpectRows(Read(reader, fresh), {{Value(1), Value(10)}, {Value(2), Value(20)}});
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: branch selectors", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART, Entry::CATALOG);
	const auto selector =
	    GENERATE("'scan.snapshot-id'='1'", "'scan.timestamp-millis'='4102444800000'", "'scan.timestamp'='2099-01-01'");
	CAPTURE(static_cast<int>(entry), selector, warehouse);
	SQL(writer, "CREATE TABLE pm.race.selected_branch (id INTEGER, value INTEGER) WITH (" + string(selector) + ")");
	SQL(writer, "INSERT INTO pm.race.selected_branch VALUES (1,10)");
	CopyBranch(warehouse + "/race.db/selected_branch");
	SQL(writer, "INSERT INTO pm.race.selected_branch VALUES (2,20)");
	auto bound = Bind(reader, warehouse, entry, "selected_branch$branch_dev");
	REQUIRE(Count(reader, bound) == 1);
	ExpectRows(Read(reader, bound), {{Value(1), Value(10)}});
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: branch selector overrides", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART);
	CAPTURE(static_cast<int>(entry), warehouse);
	SQL(writer, "INSERT INTO pm.race.t VALUES (1,10)");
	CopyBranch(warehouse + "/race.db/t");
	SQL(writer, "INSERT INTO pm.race.t VALUES (2,20)");
	named_parameter_map_t parameters;
	parameters["snapshot_from_id"] = Value::BIGINT(1);
	auto historical = Bind(reader, warehouse, entry, "t$branch_dev", parameters);
	REQUIRE(Count(reader, historical) == 1);
	ExpectRows(Read(reader, historical), {{Value(1), Value(10)}});
	parameters["snapshot_from_id"] = Value::BIGINT(2);
	// Snapshot 2 exists on main only, with no table selector to cause a conflict.
	REQUIRE_THROWS_WITH(Bind(reader, warehouse, entry, "t$branch_dev", parameters), Catch::Contains("snapshot"));
	parameters.clear();
	parameters["snapshot_from_timestamp"] = Value("2099-01-01").DefaultCastAs(LogicalType::TIMESTAMP);
	auto timestamp = Bind(reader, warehouse, entry, "t$branch_dev", parameters);
	REQUIRE(Count(reader, timestamp) == 1);
	ExpectRows(Read(reader, timestamp), {{Value(1), Value(10)}});
	parameters["snapshot_from_timestamp"] = Value("1900-01-01").DefaultCastAs(LogicalType::TIMESTAMP);
	REQUIRE_THROWS_WITH(Bind(reader, warehouse, entry, "t$branch_dev", parameters), Catch::Contains("snapshot"));
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: missing branch and fallback", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART, Entry::CATALOG);
	CAPTURE(static_cast<int>(entry), warehouse);
	SQL(writer, "INSERT INTO pm.race.t VALUES (1,10)");
	CopyBranch(warehouse + "/race.db/t");
	// The filesystem catalog currently loads main's schema for a missing branch;
	// the branch-aware native scanner then rejects it. This must not become
	// an empty/main read, regardless of the scan entrypoint.
	const auto missing = Catch::Contains("not found latest schema");
	REQUIRE_THROWS_WITH(Bind(reader, warehouse, entry, "t$branch_missing"), missing);
	// Nonempty names are not trimmed to an existing branch.
	REQUIRE_THROWS_WITH(Bind(reader, warehouse, entry, "t$branch_ dev "), missing);
	SetSchemaOption(warehouse + "/race.db/t/schema/schema-0", "branch", " dev ");
	REQUIRE_THROWS_WITH(Bind(reader, warehouse, entry, "t"), missing);
	auto main = Bind(reader, warehouse, entry, "t$branch_main");
	REQUIRE(Count(reader, main) == 1);
	ExpectRows(Read(reader, main), {{Value(1), Value(10)}});
	SQL(writer, "CREATE TABLE pm.race.missing_option (id INTEGER) WITH ('branch'='missing')");
	REQUIRE_THROWS_WITH(Bind(reader, warehouse, entry, "missing_option"), missing);
	SQL(writer, "CREATE TABLE pm.race.fallback (id INTEGER) WITH ('scan.fallback-branch'='main')");
	REQUIRE_THROWS_WITH(Bind(reader, warehouse, entry, "fallback"),
	                    Catch::Contains("does not support a non-empty scan.fallback-branch"));
	SQL(reader, "SELECT 42");
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: hidden manifests", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART, Entry::CATALOG);
	const auto selector = GENERATE("", "'scan.snapshot-id'='1'", "'scan.timestamp-millis'='4102444800000'",
	                               "'scan.timestamp'='2099-01-01'");
	CAPTURE(static_cast<int>(entry), selector, warehouse);
	auto options = string(selector).empty() ? "" : " WITH (" + string(selector) + ")";
	SQL(writer, "CREATE TABLE pm.race.hidden (id INTEGER, value INTEGER)" + options);
	SQL(writer, "INSERT INTO pm.race.hidden VALUES (1,10)");
	std::filesystem::rename(warehouse + "/race.db/hidden/manifest", warehouse + "/hidden-manifest");
	auto bound = Bind(reader, warehouse, entry, "hidden");
	REQUIRE_THROWS_WITH(Count(reader, bound), Catch::Contains("Not exist"));
	REQUIRE_THROWS_WITH(Read(reader, bound), Catch::Contains("Not exist"));
	SQL(reader, "SELECT 42");
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: corrupt snapshot metadata", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART, Entry::CATALOG);
	CAPTURE(static_cast<int>(entry), warehouse);
	SQL(writer, "INSERT INTO pm.race.t VALUES (1,10)");
	SQL(writer, "INSERT INTO pm.race.t VALUES (2,20)");
	// ListSnapshots reads retained history, not just the newest snapshot.
	const auto path = warehouse + "/race.db/t/snapshot/snapshot-1";
	REQUIRE(std::filesystem::exists(path));
	{
		std::ofstream corrupt(path, std::ios::trunc);
		corrupt << "not valid snapshot JSON";
		corrupt.close();
		REQUIRE_FALSE(corrupt.fail());
	}
	REQUIRE_THROWS_WITH(Bind(reader, warehouse, entry), Catch::Contains("deserialize failed"));
	SQL(reader, "SELECT 42");
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: mode", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART, Entry::CATALOG);
	CAPTURE(static_cast<int>(entry), warehouse);
	SQL(writer, "CREATE TABLE pm.race.mode (id INTEGER) WITH ('scan.mode'='latest','scan.snapshot-id'='1')");
	REQUIRE_THROWS_WITH(Bind(reader, warehouse, entry, "mode"), Catch::Contains("scan.mode conflicts"));
	const auto missing = GENERATE("from-snapshot", "from-snapshot-full", "from-timestamp");
	SQL(writer, "CREATE TABLE pm.race.missing_selector (id INTEGER) WITH ('scan.mode'='" + string(missing) + "')");
	REQUIRE_THROWS_WITH(Bind(reader, warehouse, entry, "missing_selector"), Catch::Contains("requires a matching"));
	if (entry != Entry::CATALOG) {
		SQL(writer, "CREATE TABLE pm.race.conflicting_time (id INTEGER) WITH ('scan.timestamp'='2099-01-01')");
		named_parameter_map_t parameters;
		parameters["snapshot_from_timestamp"] = Value("2099-01-01").DefaultCastAs(LogicalType::TIMESTAMP);
		REQUIRE_THROWS_WITH(Bind(reader, warehouse, entry, "conflicting_time", parameters),
		                    Catch::Contains("cannot be set at the same time"));
	}
	SQL(reader, "SELECT 42");
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: unsupported", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART, Entry::CATALOG);
	CAPTURE(static_cast<int>(entry), warehouse);
	SQL(writer, "CREATE TABLE pm.race.unsupported (id INTEGER PRIMARY KEY)");
	auto bound = Bind(reader, warehouse, entry, "unsupported");
	REQUIRE_THROWS_WITH(Count(reader, bound), Catch::Contains("do not support pk table bucket=-1"));
	REQUIRE_THROWS_WITH(Read(reader, bound), Catch::Contains("do not support pk table bucket=-1"));
	PushEqual(reader, bound, 0, 1);
	REQUIRE_THROWS_WITH(Count(reader, bound), Catch::Contains("do not support pk table bucket=-1"));
	REQUIRE_THROWS_WITH(Read(reader, bound), Catch::Contains("do not support pk table bucket=-1"));
	// Persistent selectors also exercise the attached path. Failure must
	// occur in Bind, not merely when a later reader sees the saved error.
	SQL(writer, "CREATE TABLE pm.race.unsupported_id (id INTEGER PRIMARY KEY) WITH ('scan.snapshot-id'='1')");
	REQUIRE_THROWS_WITH(Bind(reader, warehouse, entry, "unsupported_id"),
	                    Catch::Contains("do not support pk table bucket=-1"));
	SQL(writer, "CREATE TABLE pm.race.unsupported_time (id INTEGER PRIMARY KEY) WITH ('scan.timestamp-millis'='1')");
	REQUIRE_THROWS_WITH(Bind(reader, warehouse, entry, "unsupported_time"),
	                    Catch::Contains("do not support pk table bucket=-1"));
	SQL(reader, "SELECT 42");
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: pk", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART, Entry::CATALOG);
	CAPTURE(static_cast<int>(entry), warehouse);
	SQL(writer, "CREATE TABLE pm.race.pk (id INTEGER PRIMARY KEY, value INTEGER) WITH ('bucket'='1')");
	SQL(writer, "INSERT INTO pm.race.pk VALUES (1,10),(2,20)");
	auto bound = Bind(reader, warehouse, entry, "pk");
	SQL(writer, "INSERT INTO pm.race.pk VALUES (1,99),(3,30)");
	ExpectRows(Read(reader, bound), {{Value(1), Value(10)}, {Value(2), Value(20)}});
	INFO("PK statistics advanced");
	REQUIRE(Count(reader, bound) == 2);
	auto fresh = Bind(reader, warehouse, entry, "pk");
	ExpectRows(Read(reader, fresh), {{Value(1), Value(99)}, {Value(2), Value(20)}, {Value(3), Value(30)}});
	SQL(reader, "SELECT 42");
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: partition", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART, Entry::CATALOG);
	CAPTURE(static_cast<int>(entry), warehouse);
	SQL(writer, "CREATE TABLE pm.race.partitioned (id INTEGER, part INTEGER) PARTITIONED BY (part)");
	SQL(writer, "INSERT INTO pm.race.partitioned VALUES (1,10),(2,20)");
	auto bound = Bind(reader, warehouse, entry, "partitioned");
	PushEqual(reader, bound, 1, 10);
	SQL(writer, "INSERT INTO pm.race.partitioned VALUES (3,10),(4,20)");
	ExpectRows(Read(reader, bound), {{Value(1), Value(10)}});
	auto fresh = Bind(reader, warehouse, entry, "partitioned");
	PushEqual(reader, fresh, 1, 10);
	ExpectRows(Read(reader, fresh), {{Value(1), Value(10)}, {Value(3), Value(10)}});
	SQL(reader, "SELECT 42");
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: pk_index", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART, Entry::CATALOG);
	CAPTURE(static_cast<int>(entry), warehouse);
	const auto path = warehouse + "/race.db/pk_index";
	std::filesystem::copy(native_fixtures + "/parquet/pk_btree_e2e.db/pk_btree_e2e", path,
	                      std::filesystem::copy_options::recursive);
	std::filesystem::rename(path + "/snapshot/LATEST", warehouse + "/staged-latest");
	for (int id = 3; id <= 5; id++) {
		std::filesystem::rename(path + "/snapshot/snapshot-" + std::to_string(id),
		                        warehouse + "/staged-snapshot-" + std::to_string(id));
	}
	auto bound = Bind(reader, warehouse, entry, "pk_index");
	PushEqual(reader, bound, 1, 0);
	Rows expected;
	for (int id = 10; id <= 2000; id += 10) {
		expected.push_back({Value(id), Value(0), Value("keep")});
	}
	std::sort(expected.begin(), expected.end(),
	          [](const vector<Value> &a, const vector<Value> &b) { return a[0].ToString() < b[0].ToString(); });
	// No DuckDB residual evaluation here: exact rows prove index row-range pruning.
	ExpectRows(Read(reader, bound), expected);
	for (int id = 3; id <= 5; id++) {
		std::filesystem::rename(warehouse + "/staged-snapshot-" + std::to_string(id),
		                        path + "/snapshot/snapshot-" + std::to_string(id));
	}
	ExpectRows(Read(reader, bound), expected);
	auto fresh = Bind(reader, warehouse, entry, "pk_index");
	PushEqual(reader, fresh, 1, 0);
	expected.erase(std::remove_if(expected.begin(), expected.end(),
	                              [](const vector<Value> &row) {
		                              return row[0].GetValue<int32_t>() == 10 || row[0].GetValue<int32_t>() == 20;
	                              }),
	               expected.end());
	expected.push_back({Value(2001), Value(0), Value("keep")});
	std::sort(expected.begin(), expected.end(),
	          [](const vector<Value> &a, const vector<Value> &b) { return a[0].ToString() < b[0].ToString(); });
	ExpectRows(Read(reader, fresh), expected);
	SQL(reader, "SELECT 42");
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: metadata", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART, Entry::CATALOG);
	CAPTURE(static_cast<int>(entry), warehouse);
	CheckColumnSignatures();
	const auto path = warehouse + "/race.db/metadata";
	std::filesystem::copy(fixtures + "/scalar_index.db/t1", path, std::filesystem::copy_options::recursive);
	auto bound = Bind(reader, warehouse, entry, "metadata");
	auto before = Read(reader, bound);
	INFO("metadata fixture row count");
	REQUIRE(before.size() == 30);
	std::filesystem::copy_file(schema_fixtures + "/metadata-schema-1.json", path + "/schema/schema-1");
	auto fresh = Bind(reader, warehouse, entry, "metadata");
	ExpectRows(Read(reader, fresh), before);
	ExpectRows(Read(reader, bound), before);
	SQL(reader, "SELECT 42");
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: empty", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART, Entry::CATALOG);
	CAPTURE(static_cast<int>(entry), warehouse);
	auto empty = Bind(reader, warehouse, entry);
	SQL(writer, "INSERT INTO pm.race.t VALUES (1,10),(2,20)");
	INFO("empty binding statistics advanced");
	REQUIRE(Count(reader, empty) == 0);
	ExpectRows(Read(reader, empty), {});
	auto fresh = Bind(reader, warehouse, entry);
	ExpectRows(Read(reader, fresh), {{Value(1), Value(10)}, {Value(2), Value(20)}});
	SQL(reader, "SELECT 42");
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: retained", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART, Entry::CATALOG);
	const auto stats_first = GENERATE(true, false);
	CAPTURE(static_cast<int>(entry), stats_first, warehouse);
	SQL(writer, "INSERT INTO pm.race.t VALUES (1,10),(2,20)");
	auto bound = Bind(reader, warehouse, entry);
	if (stats_first) {
		INFO("S1 count before commit");
		REQUIRE(Count(reader, bound) == 2);
	}
	SQL(writer, "INSERT INTO pm.race.t VALUES (3,30)");
	if (!stats_first) {
		ExpectRows(Read(reader, bound), {{Value(1), Value(10)}, {Value(2), Value(20)}});
	}
	INFO("bound statistics advanced to S2");
	REQUIRE(Count(reader, bound) == 2);
	ExpectRows(Read(reader, bound), {{Value(1), Value(10)}, {Value(2), Value(20)}});
	ExpectRows(Read(reader, bound), {{Value(1), Value(10)}, {Value(2), Value(20)}});
	auto fresh = Bind(reader, warehouse, entry);
	ExpectRows(Read(reader, fresh), {{Value(1), Value(10)}, {Value(2), Value(20)}, {Value(3), Value(30)}});
	SQL(reader, "SELECT 42");
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: expiry", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART, Entry::CATALOG);
	CAPTURE(static_cast<int>(entry), warehouse);
	SQL(writer, "INSERT INTO pm.race.t VALUES (1,10)");
	auto bound = Bind(reader, warehouse, entry);
	SQL(writer, "INSERT INTO pm.race.t VALUES (2,20)");
	// Fault injection only in this disposable warehouse, not a retention implementation.
	std::filesystem::remove(warehouse + "/race.db/t/snapshot/snapshot-1");
	REQUIRE_THROWS_WITH(Count(reader, bound), Catch::Contains("snapshot"));
	REQUIRE_THROWS_WITH(Read(reader, bound), Catch::Contains("snapshot"));
	SQL(reader, "SELECT 42");
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: partial", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART, Entry::CATALOG);
	CAPTURE(static_cast<int>(entry), warehouse);
	SQL(writer, "INSERT INTO pm.race.t SELECT i::INTEGER,i::INTEGER FROM range(5000) r(i)");
	auto bound = Bind(reader, warehouse, entry);
	auto partial = Read(reader, bound, true);
	INFO("partial reader must leave unconsumed rows");
	REQUIRE((!partial.empty() && partial.size() < 5000));
	auto rows = Read(reader, bound);
	INFO("fresh reader shared a consumed cursor");
	REQUIRE(rows.size() == 5000);
	vector<bool> seen(5000, false);
	for (auto &row : rows) {
		INFO("partial-reader result types changed");
		REQUIRE((row.size() == 2 && row[0].type() == LogicalType::INTEGER && row[1].type() == LogicalType::INTEGER));
		auto id = row[0].GetValue<int32_t>();
		INFO("fresh reader returned a missing, duplicate or incorrect row");
		REQUIRE((id >= 0 && id < 5000 && !seen[id] && row[1].GetValue<int32_t>() == id));
		seen[id] = true;
	}
	SQL(reader, "SELECT 42");
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: schema", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART, Entry::CATALOG);
	CAPTURE(static_cast<int>(entry), warehouse);
	const auto path = warehouse + "/race.db/evolution";
	std::filesystem::copy(fixtures + "/schema_evolution.db/mixed_types", path,
	                      std::filesystem::copy_options::recursive);
	// Publish genuine fixture metadata in a controlled order. This is fault
	// injection for schema visibility, not a claim to test the writer protocol.
	const auto published = path + "/schema/schema-1";
	const auto staged = warehouse + "/staged-schema-1";
	std::filesystem::rename(published, staged);
	auto bound = Bind(reader, warehouse, entry, "evolution");
	ExpectRows(Read(reader, bound), {{Value(1), Value("before"), Value::BIGINT(10)}});
	std::filesystem::rename(staged, published);
	ExpectRows(Read(reader, bound), {{Value(1), Value("before"), Value::BIGINT(10)}});
	if (entry == Entry::CATALOG) {
		REQUIRE_THROWS_WITH(Bind(reader, warehouse, entry, "evolution"), Catch::Contains("detach and reattach"));
	}
	SQL(reader, "DETACH pm");
	ExpectRows(Read(reader, bound), {{Value(1), Value("before"), Value::BIGINT(10)}});
	SQL(reader, "ATTACH " + Value(warehouse).ToSQLString() + " AS pm (TYPE paimon)");
	auto fresh = Bind(reader, warehouse, entry, "evolution");
	ExpectRows(Read(reader, fresh), {{Value("before"), Value(1), Value::BIGINT(10)}});
	SQL(reader, "SELECT 42");
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: indexed_schema", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART, Entry::CATALOG);
	CAPTURE(static_cast<int>(entry), warehouse);
	const auto path = warehouse + "/race.db/indexed";
	std::filesystem::copy(fixtures + "/scalar_index.db/t1", path, std::filesystem::copy_options::recursive);
	// Synthetic schema visibility: add a nullable INT, then exchange its
	// name with the indexed INT. Data and index bytes are never rewritten.
	std::filesystem::copy_file(schema_fixtures + "/index-schema-1.json", path + "/schema/schema-1");
	auto bound = Bind(reader, warehouse, entry, "indexed");
	INFO("unexpected indexed fixture schema");
	REQUIRE(bound.names == vector<string> {"idx", "payload", "part", "other"});
	LogicalGet get(0, bound.function, nullptr, bound.types, bound.names);
	for (idx_t i = 0; i < bound.types.size(); i++) {
		get.AddColumnId(i);
	}
	auto predicate = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NULL, LogicalType::BOOLEAN);
	predicate->children.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(0, 3)));
	vector<unique_ptr<Expression>> filters;
	filters.push_back(std::move(predicate));
	bound.function.pushdown_complex_filter(*reader.context, get, bound.data.get(), filters);
	INFO("DuckDB residual filter was removed");
	REQUIRE(filters.size() == 1);
	Rows expected;
	const vector<vector<int>> ids {{0, 10, 20, 30, 40, 50, 60, 70, 80, 90},
	                               {0, 15, 25, 35, 45, 55, 65, 75, 85, 90},
	                               {0, 12, 22, 32, 42, 52, 62, 72, 82, 90}};
	const vector<string> prefixes {"hit_", "miss_a_", "miss_b_"};
	for (idx_t part = 0; part < ids.size(); part++) {
		for (auto id : ids[part]) {
			auto payload = prefixes[part] + (id == 0 ? "00" : std::to_string(id));
			expected.push_back(
			    {Value(id), Value(payload), Value(static_cast<int32_t>(part)), Value(LogicalType::INTEGER)});
		}
	}
	auto order = [](const vector<Value> &a, const vector<Value> &b) {
		if (a[0].ToString() != b[0].ToString()) {
			return a[0].ToString() < b[0].ToString();
		}
		return a[2].GetValue<int32_t>() < b[2].GetValue<int32_t>();
	};
	std::sort(expected.begin(), expected.end(), order);
	auto before = Read(reader, bound);
	std::sort(before.begin(), before.end(), order);
	ExpectRows(before, expected);
	std::filesystem::copy_file(schema_fixtures + "/index-schema-2.json", path + "/schema/schema-2");
	auto after = Read(reader, bound);
	std::sort(after.begin(), after.end(), order);
	ExpectRows(after, expected);
	SQL(reader, "SELECT 42");
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: timestamp", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART);
	CAPTURE(static_cast<int>(entry), warehouse);
	SQL(writer, "INSERT INTO pm.race.t VALUES (1,10)");
	named_parameter_map_t parameters;
	parameters["snapshot_from_timestamp"] = Value("2099-01-01").DefaultCastAs(LogicalType::TIMESTAMP);
	auto bound = Bind(reader, warehouse, entry, "t", parameters);
	SQL(writer, "INSERT INTO pm.race.t VALUES (2,20)");
	ExpectRows(Read(reader, bound), {{Value(1), Value(10)}});
	auto fresh = Bind(reader, warehouse, entry, "t", parameters);
	ExpectRows(Read(reader, fresh), {{Value(1), Value(10)}, {Value(2), Value(20)}});
	SQL(reader, "SELECT 42");
}

TEST_CASE_METHOD(BindingFixture, "Paimon binding: retained table selectors", "[paimon][snapshot_binding]") {
	const auto entry = GENERATE(Entry::PATH, Entry::THREE_PART, Entry::CATALOG);
	const auto selector =
	    GENERATE("'scan.snapshot-id'='1'", "'scan.timestamp-millis'='4102444800000'", "'scan.timestamp'='2099-01-01'");
	CAPTURE(static_cast<int>(entry), selector, warehouse);
	SQL(writer, "CREATE TABLE pm.race.selected (id INTEGER, value INTEGER) WITH (" + string(selector) + ")");
	SQL(writer, "INSERT INTO pm.race.selected VALUES (1,10)");
	auto bound = Bind(reader, warehouse, entry, "selected");
	SQL(writer, "INSERT INTO pm.race.selected VALUES (2,20)");
	REQUIRE(Count(reader, bound) == 1);
	ExpectRows(Read(reader, bound), {{Value(1), Value(10)}});
	auto fresh = Bind(reader, warehouse, entry, "selected");
	if (string(selector).find("scan.snapshot-id") != string::npos) {
		ExpectRows(Read(reader, fresh), {{Value(1), Value(10)}});
	} else {
		ExpectRows(Read(reader, fresh), {{Value(1), Value(10)}, {Value(2), Value(20)}});
	}
	SQL(reader, "SELECT 42");
}
