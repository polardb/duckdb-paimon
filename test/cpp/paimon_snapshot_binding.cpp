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

#include <algorithm>
#include <filesystem>
#include <iostream>

using namespace duckdb;

static void Require(bool condition, const string &message) {
	if (!condition) {
		throw std::runtime_error(message);
	}
}

static void SQL(Connection &connection, const string &sql) {
	auto result = connection.Query(sql);
	Require(!result->HasError(), result->HasError() ? result->GetError() : "");
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
		Require(!bound.data->SupportStatementCache(), "all scan entrypoints must require rebinding");
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
	Require(stats.size() == 1 && stats[0].count_type == CountType::COUNT_EXACT, "exact statistics missing");
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
	Require(actual.size() == expected.size(), "unexpected row count: " + std::to_string(actual.size()));
	for (idx_t r = 0; r < expected.size(); r++) {
		Require(actual[r].size() == expected[r].size(), "unexpected column count");
		for (idx_t c = 0; c < expected[r].size(); c++) {
			Require(actual[r][c].type() == expected[r][c].type() &&
			            Value::NotDistinctFrom(actual[r][c], expected[r][c]),
			        "unexpected value or SQL type");
		}
	}
}

static void ExpectError(const std::function<void()> &action, const string &message) {
	try {
		action();
	} catch (const std::exception &error) {
		Require(string(error.what()).find(message) != string::npos, "unexpected error: " + string(error.what()));
		return;
	}
	throw std::runtime_error("expected error containing: " + message);
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
	Require(filters.size() == 1, "DuckDB residual filter was removed");
}

static void CheckColumnSignatures() {
	auto signature = [](const string &fields, const string &keys = "[]", const string &partitions = "[]") {
		return PaimonFunctions::GetColumnSignature("{\"fields\":" + fields + ",\"partitionKeys\":" + partitions +
		                                           ",\"primaryKeys\":" + keys + "}");
	};
	const string fields = R"([{"id":0,"name":"v","type":"INT"}])";
	auto baseline = signature(fields);
	Require(baseline != signature(R"([{"id":1,"name":"v","type":"INT"}])"), "field identity was ignored");
	Require(baseline != signature(R"([{"id":0,"name":"v","type":"INT NOT NULL"}])"), "nullability was ignored");
	Require(baseline != signature(fields, "[\"v\"]"), "primary key was ignored");
	Require(baseline != signature(fields, "[]", "[\"v\"]"), "partition key was ignored");
	const string nested = R"([{"id":0,"name":"r","type":{"type":"ROW","fields":[{"id":1,"name":"v","type":"INT"}]}}])";
	const string annotated =
	    R"([{"id":0,"name":"r","type":{"type":"ROW","fields":[{"id":1,"name":"v","type":"INT","description":"new comment"}]}}])";
	const string changed = R"([{"id":0,"name":"r","type":{"type":"ROW","fields":[{"id":2,"name":"v","type":"INT"}]}}])";
	Require(signature(nested) == signature(annotated), "nested comments changed column identity");
	Require(signature(nested) != signature(changed), "nested field identity was ignored");
}

static void Check(const string &warehouse, Entry entry, const string &scenario, const string &fixtures,
                  const string &schema_fixtures, const string &native_fixtures) {
	DuckDB db(nullptr);
	db.LoadStaticExtension<PaimonExtension>();
	Connection reader(db), writer(db);
	SQL(reader, "ATTACH " + Value(warehouse).ToSQLString() + " AS pm (TYPE paimon)");
	SQL(writer, "CREATE SCHEMA pm.race");
	SQL(writer, "CREATE TABLE pm.race.t (id INTEGER, value INTEGER)");
	if (scenario == "bind_error") {
		SQL(writer, "INSERT INTO pm.race.t VALUES (1,10)");
		std::filesystem::rename(warehouse + "/race.db/t/manifest", warehouse + "/hidden-manifest");
		ExpectError([&] { Bind(reader, warehouse, entry); }, "Not exist");
	} else if (scenario == "mode") {
		SQL(writer, "CREATE TABLE pm.race.mode (id INTEGER) WITH ('scan.mode'='latest','scan.snapshot-id'='1')");
		ExpectError([&] { Bind(reader, warehouse, entry, "mode"); }, "scan.mode conflicts");
	} else if (scenario == "unsupported") {
		SQL(writer, "CREATE TABLE pm.race.unsupported (id INTEGER PRIMARY KEY)");
		auto bound = Bind(reader, warehouse, entry, "unsupported");
		ExpectError([&] { Count(reader, bound); }, "do not support pk table bucket=-1");
		ExpectError([&] { Read(reader, bound); }, "do not support pk table bucket=-1");
		PushEqual(reader, bound, 0, 1);
		ExpectError([&] { Count(reader, bound); }, "do not support pk table bucket=-1");
		ExpectError([&] { Read(reader, bound); }, "do not support pk table bucket=-1");
		// Persistent selectors also exercise the attached path. Failure must
		// occur in Bind, not merely when a later reader sees the saved error.
		SQL(writer, "CREATE TABLE pm.race.unsupported_id (id INTEGER PRIMARY KEY) WITH ('scan.snapshot-id'='1')");
		ExpectError([&] { Bind(reader, warehouse, entry, "unsupported_id"); }, "do not support pk table bucket=-1");
		SQL(writer,
		    "CREATE TABLE pm.race.unsupported_time (id INTEGER PRIMARY KEY) WITH ('scan.timestamp-millis'='1')");
		ExpectError([&] { Bind(reader, warehouse, entry, "unsupported_time"); }, "do not support pk table bucket=-1");
	} else if (scenario == "pk") {
		SQL(writer, "CREATE TABLE pm.race.pk (id INTEGER PRIMARY KEY, value INTEGER) WITH ('bucket'='1')");
		SQL(writer, "INSERT INTO pm.race.pk VALUES (1,10),(2,20)");
		auto bound = Bind(reader, warehouse, entry, "pk");
		SQL(writer, "INSERT INTO pm.race.pk VALUES (1,99),(3,30)");
		ExpectRows(Read(reader, bound), {{Value(1), Value(10)}, {Value(2), Value(20)}});
		Require(Count(reader, bound) == 2, "PK statistics advanced");
		auto fresh = Bind(reader, warehouse, entry, "pk");
		ExpectRows(Read(reader, fresh), {{Value(1), Value(99)}, {Value(2), Value(20)}, {Value(3), Value(30)}});
	} else if (scenario == "partition") {
		SQL(writer, "CREATE TABLE pm.race.partitioned (id INTEGER, part INTEGER) PARTITIONED BY (part)");
		SQL(writer, "INSERT INTO pm.race.partitioned VALUES (1,10),(2,20)");
		auto bound = Bind(reader, warehouse, entry, "partitioned");
		PushEqual(reader, bound, 1, 10);
		SQL(writer, "INSERT INTO pm.race.partitioned VALUES (3,10),(4,20)");
		ExpectRows(Read(reader, bound), {{Value(1), Value(10)}});
		auto fresh = Bind(reader, warehouse, entry, "partitioned");
		PushEqual(reader, fresh, 1, 10);
		ExpectRows(Read(reader, fresh), {{Value(1), Value(10)}, {Value(3), Value(10)}});
	} else if (scenario == "pk_index") {
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
	} else if (scenario == "metadata") {
		CheckColumnSignatures();
		const auto path = warehouse + "/race.db/metadata";
		std::filesystem::copy(fixtures + "/scalar_index.db/t1", path, std::filesystem::copy_options::recursive);
		auto bound = Bind(reader, warehouse, entry, "metadata");
		auto before = Read(reader, bound);
		Require(before.size() == 30, "metadata fixture row count");
		std::filesystem::copy_file(schema_fixtures + "/metadata-schema-1.json", path + "/schema/schema-1");
		auto fresh = Bind(reader, warehouse, entry, "metadata");
		ExpectRows(Read(reader, fresh), before);
		ExpectRows(Read(reader, bound), before);
	} else if (scenario == "branch") {
		ExpectError([&] { Bind(reader, warehouse, entry, "t$branch_audit"); }, "supports only the default branch");
	} else if (scenario == "empty") {
		auto empty = Bind(reader, warehouse, entry);
		SQL(writer, "INSERT INTO pm.race.t VALUES (1,10),(2,20)");
		Require(Count(reader, empty) == 0, "empty binding statistics advanced");
		ExpectRows(Read(reader, empty), {});
		auto fresh = Bind(reader, warehouse, entry);
		ExpectRows(Read(reader, fresh), {{Value(1), Value(10)}, {Value(2), Value(20)}});
	} else if (scenario == "retained" || scenario == "retained_rows") {
		SQL(writer, "INSERT INTO pm.race.t VALUES (1,10),(2,20)");
		auto bound = Bind(reader, warehouse, entry);
		if (scenario == "retained") {
			Require(Count(reader, bound) == 2, "S1 count before commit");
		}
		SQL(writer, "INSERT INTO pm.race.t VALUES (3,30)");
		if (scenario == "retained_rows") {
			ExpectRows(Read(reader, bound), {{Value(1), Value(10)}, {Value(2), Value(20)}});
		}
		Require(Count(reader, bound) == 2, "bound statistics advanced to S2");
		ExpectRows(Read(reader, bound), {{Value(1), Value(10)}, {Value(2), Value(20)}});
		ExpectRows(Read(reader, bound), {{Value(1), Value(10)}, {Value(2), Value(20)}});
		auto fresh = Bind(reader, warehouse, entry);
		ExpectRows(Read(reader, fresh), {{Value(1), Value(10)}, {Value(2), Value(20)}, {Value(3), Value(30)}});
	} else if (scenario == "expiry") {
		SQL(writer, "INSERT INTO pm.race.t VALUES (1,10)");
		auto bound = Bind(reader, warehouse, entry);
		SQL(writer, "INSERT INTO pm.race.t VALUES (2,20)");
		// Fault injection only in this disposable warehouse, not a retention implementation.
		std::filesystem::remove(warehouse + "/race.db/t/snapshot/snapshot-1");
		ExpectError([&] { Count(reader, bound); }, "snapshot");
		ExpectError([&] { Read(reader, bound); }, "snapshot");
	} else if (scenario == "partial") {
		SQL(writer, "INSERT INTO pm.race.t SELECT i::INTEGER,i::INTEGER FROM range(5000) r(i)");
		auto bound = Bind(reader, warehouse, entry);
		auto partial = Read(reader, bound, true);
		Require(!partial.empty() && partial.size() < 5000, "partial reader must leave unconsumed rows");
		auto rows = Read(reader, bound);
		Require(rows.size() == 5000, "fresh reader shared a consumed cursor");
		vector<bool> seen(5000, false);
		for (auto &row : rows) {
			Require(row.size() == 2 && row[0].type() == LogicalType::INTEGER && row[1].type() == LogicalType::INTEGER,
			        "partial-reader result types changed");
			auto id = row[0].GetValue<int32_t>();
			Require(id >= 0 && id < 5000 && !seen[id] && row[1].GetValue<int32_t>() == id,
			        "fresh reader returned a missing, duplicate or incorrect row");
			seen[id] = true;
		}
	} else if (scenario == "schema") {
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
			ExpectError([&] { Bind(reader, warehouse, entry, "evolution"); }, "detach and reattach");
		}
		SQL(reader, "DETACH pm");
		ExpectRows(Read(reader, bound), {{Value(1), Value("before"), Value::BIGINT(10)}});
		SQL(reader, "ATTACH " + Value(warehouse).ToSQLString() + " AS pm (TYPE paimon)");
		auto fresh = Bind(reader, warehouse, entry, "evolution");
		ExpectRows(Read(reader, fresh), {{Value("before"), Value(1), Value::BIGINT(10)}});
	} else if (scenario == "indexed_schema") {
		const auto path = warehouse + "/race.db/indexed";
		std::filesystem::copy(fixtures + "/scalar_index.db/t1", path, std::filesystem::copy_options::recursive);
		// Synthetic schema visibility: add a nullable INT, then exchange its
		// name with the indexed INT. Data and index bytes are never rewritten.
		std::filesystem::copy_file(schema_fixtures + "/index-schema-1.json", path + "/schema/schema-1");
		auto bound = Bind(reader, warehouse, entry, "indexed");
		Require(bound.names == vector<string> {"idx", "payload", "part", "other"}, "unexpected indexed fixture schema");
		LogicalGet get(0, bound.function, nullptr, bound.types, bound.names);
		for (idx_t i = 0; i < bound.types.size(); i++) {
			get.AddColumnId(i);
		}
		auto predicate = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NULL, LogicalType::BOOLEAN);
		predicate->children.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(0, 3)));
		vector<unique_ptr<Expression>> filters;
		filters.push_back(std::move(predicate));
		bound.function.pushdown_complex_filter(*reader.context, get, bound.data.get(), filters);
		Require(filters.size() == 1, "DuckDB residual filter was removed");
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
	} else if (scenario == "timestamp") {
		SQL(writer, "INSERT INTO pm.race.t VALUES (1,10)");
		named_parameter_map_t parameters;
		parameters["snapshot_from_timestamp"] = Value("2099-01-01").DefaultCastAs(LogicalType::TIMESTAMP);
		auto bound = Bind(reader, warehouse, entry, "t", parameters);
		SQL(writer, "INSERT INTO pm.race.t VALUES (2,20)");
		ExpectRows(Read(reader, bound), {{Value(1), Value(10)}});
		auto fresh = Bind(reader, warehouse, entry, "t", parameters);
		ExpectRows(Read(reader, fresh), {{Value(1), Value(10)}, {Value(2), Value(20)}});
	} else {
		throw std::runtime_error("unknown scenario");
	}
	SQL(reader, "SELECT 42");
}

int main(int argc, char **argv) {
	try {
		Require(argc == 5, "usage: paimon_snapshot_binding_test <new-temporary-directory> <fixture-directory> "
		                   "<schema-fixtures> <native-fixtures>");
		const string root = argv[1];
		Require(!std::filesystem::exists(root), "test directory must not already exist");
		std::filesystem::create_directories(root);
		idx_t failures = 0;
		for (auto entry : {Entry::PATH, Entry::THREE_PART, Entry::CATALOG}) {
			for (const string scenario :
			     {"branch", "empty", "retained", "retained_rows", "expiry", "partial", "schema", "indexed_schema",
			      "timestamp", "unsupported", "pk", "partition", "pk_index", "metadata", "mode", "bind_error"}) {
				if (entry == Entry::CATALOG && scenario == "timestamp") {
					continue; // Catalog AT selectors are covered by SQLLogicTests.
				}
				const auto name = std::to_string(static_cast<int>(entry)) + "-" + scenario;
				try {
					Check(root + "/" + name, entry, scenario, argv[2], argv[3], argv[4]);
					std::cout << "PASS " << name << std::endl;
				} catch (const std::exception &error) {
					failures++;
					std::cerr << "FAIL " << name << ": " << error.what() << std::endl;
				}
			}
		}
		return failures ? 1 : 0;
	} catch (const std::exception &error) {
		std::cerr << error.what() << std::endl;
		return 1;
	}
}
