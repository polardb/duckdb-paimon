// Copyright (c) 2026, Alibaba Group Holding Limited
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "paimon_extension.hpp"
#include "paimon_functions.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "httplib.hpp"
#include "yyjson.hpp"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <thread>

#if !PAIMON_ENABLE_REST
#error REST branch tests require PAIMON_ENABLE_REST
#endif

namespace {

using namespace duckdb;

std::string JsonString(const std::string &value) {
	using namespace duckdb_yyjson;
	std::unique_ptr<yyjson_mut_doc, decltype(&yyjson_mut_doc_free)> doc(yyjson_mut_doc_new(nullptr),
	                                                                    yyjson_mut_doc_free);
	if (!doc) {
		throw std::bad_alloc();
	}
	yyjson_mut_doc_set_root(doc.get(), yyjson_mut_strcpy(doc.get(), value.c_str()));
	std::unique_ptr<char, decltype(&std::free)> text(yyjson_mut_write(doc.get(), 0, nullptr), std::free);
	if (!text) {
		throw std::bad_alloc();
	}
	return text.get();
}

std::string SchemaJson(bool dev, bool option_dev) {
	return std::string(R"({"version":3,"id":0,"highestFieldId":0,"fields":[{"id":0,"name":)") +
	       (dev ? R"("dev_value","type":"STRING")" : R"("main_value","type":"INT")") +
	       R"(}],"partitionKeys":[],"primaryKeys":[],"options":{"bucket":"-1","file.format":"parquet")" +
	       (option_dev ? R"(,"branch":"dev")" : "") + R"(},"timeMillis":123})";
}

struct RestBoundScan {
	BoundTableFunction function;
	unique_ptr<FunctionData> data;
	vector<LogicalType> types;
	vector<Identifier> names;
};

struct RestBranchFixture {
	DuckDB db;
	Connection connection;
	std::filesystem::path root;
	duckdb_httplib::Server server;
	std::thread listener;
	std::mutex mutex;
	std::vector<std::string> requests;
	std::vector<std::string> unexpected;
	std::atomic<bool> listen_ok {false};
	std::atomic<bool> finished {false};
	std::string uri;
	const std::string base = "/v1/databases/db/tables/t";

	RestBranchFixture(bool option_dev, bool dev, bool fail_snapshots = false, int branch_schema_status = 200)
	    : db(nullptr), connection(db) {
		db.LoadStaticExtension<PaimonExtension>();
		static std::atomic<uint64_t> next_id {0};
		const auto *test_root = std::getenv("PAIMON_TEST_ROOT");
		auto parent = test_root ? std::filesystem::path(test_root) : std::filesystem::temp_directory_path();
		const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
		root = parent / ("paimon-rest-branch-" + std::to_string(stamp) + "-" + std::to_string(next_id++));
		REQUIRE(std::filesystem::create_directories(root));
		const auto main_path = root / "main";
		const auto dev_path = root / "dev";
		// Non-main native scanners reload schema from disk even for an empty REST table.
		const auto schema_path = dev_path / "branch" / "branch-dev" / "schema";
		REQUIRE(std::filesystem::create_directories(schema_path));
		std::ofstream schema_file(schema_path / "schema-0");
		REQUIRE(schema_file.good());
		schema_file << SchemaJson(true, false);
		schema_file.close();
		REQUIRE(schema_file.good());
		const auto main_response = TableResponse("t", main_path.string(), SchemaJson(false, option_dev));
		const auto dev_response = TableResponse("t$branch_dev", dev_path.string(), SchemaJson(true, false));
		const auto selected = dev ? base + "$branch_dev" : base;
		server.Get(".*", [this, option_dev, dev, fail_snapshots, branch_schema_status, selected, main_response,
		                  dev_response](const duckdb_httplib::Request &request, duckdb_httplib::Response &response) {
			std::lock_guard<std::mutex> lock(mutex);
			requests.push_back(request.path);
			if (request.get_header_value("Authorization") != "Bearer test-token") {
				unexpected.push_back("missing test authorization: " + request.path);
			} else if (request.path == "/v1/config" && request.get_param_value("warehouse") == "branch-test") {
				response.set_content(R"({"defaults":{},"overrides":{}})", "application/json");
				return;
			} else if (branch_schema_status != 200 && request.path == "/v1/databases") {
				response.set_content(R"({"databases":["db"]})", "application/json");
				return;
			} else if (branch_schema_status != 200 && request.path == "/v1/databases/db") {
				response.set_content(R"({"name":"db","options":{}})", "application/json");
				return;
			} else if (request.path == base && (!dev || option_dev)) {
				response.set_content(main_response, "application/json");
				return;
			} else if (request.path == base + "$branch_dev" && dev) {
				response.status = branch_schema_status;
				if (branch_schema_status == 200) {
					response.set_content(dev_response, "application/json");
				} else {
					response.set_content("{\"code\":" + std::to_string(branch_schema_status) +
					                         R"(,"message":"selected branch schema unavailable"})",
					                     "application/json");
				}
				return;
			} else if (request.path == selected + "/snapshots") {
				response.status = fail_snapshots ? 400 : 200;
				response.set_content(fail_snapshots ? R"({"code":400,"message":"snapshot listing deliberately failed"})"
				                                    : R"({"snapshots":[]})",
				                     "application/json");
				return;
			} else {
				unexpected.push_back(request.path);
			}
			response.status = 400;
			response.set_content("{\"code\":400,\"message\":" +
			                         JsonString("unexpected mock request: " + request.method + " " + request.path) +
			                         "}",
			                     "application/json");
		});
		server.set_read_timeout(5);
		server.set_write_timeout(5);
		server.new_task_queue = []() {
			return new duckdb_httplib::ThreadPool(2);
		};
		const int port = server.bind_to_any_port("127.0.0.1");
		REQUIRE(port > 0);
		uri = "http://127.0.0.1:" + std::to_string(port);
		listener = std::thread([this]() {
			listen_ok = server.listen_after_bind();
			finished = true;
		});
		server.wait_until_ready();
		if (!server.is_running() || finished) {
			Stop();
			FAIL("Local REST mock failed to start");
		}
	}

	~RestBranchFixture() {
		Stop();
		std::error_code error;
		std::filesystem::remove_all(root, error);
	}

	void Stop() {
		server.stop();
		if (listener.joinable()) {
			listener.join();
		}
	}

	static std::string TableResponse(const std::string &name, const std::string &path, const std::string &schema) {
		return R"({"id":"1","name":)" + JsonString(name) + R"(,"path":)" + JsonString(path) +
		       R"(,"schemaId":0,"schema":)" + schema + R"(,"updatedAt":123})";
	}

	void Attach() {
		auto result =
		    connection.Query("ATTACH 'branch-test' AS rest_pm (TYPE paimon, READ_ONLY, METASTORE 'rest', URI " +
		                     Value(uri).ToSQLString() + ", TOKEN_PROVIDER 'bear', TOKEN 'test-token')");
		INFO((result->HasError() ? result->GetError() : ""));
		REQUIRE_FALSE(result->HasError());
	}

	void CheckPhysicalEntry() {
		connection.BeginTransaction();
		try {
			const string table_name = "t";
			EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, Identifier(table_name));
			auto &entry = Catalog::GetEntry(*connection.context, "rest_pm", "db", lookup).Cast<TableCatalogEntry>();
			const auto names = entry.GetColumns().GetColumnNames();
			const auto types = entry.GetTypes();
			REQUIRE(names.size() == 1);
			REQUIRE(names[0] == "main_value");
			REQUIRE(types.size() == 1);
			REQUIRE(types[0] == LogicalType(LogicalTypeId::INTEGER));
			connection.Commit();
		} catch (...) {
			connection.Rollback();
			throw;
		}
	}

	void CheckAttachedReadFailure() {
		auto result = connection.Query("SELECT * FROM rest_pm.db.t");
		REQUIRE(result->HasError());
		REQUIRE_THAT(result->GetError(), Catch::Contains("selected branch schema unavailable"));
	}

	RestBoundScan Bind(const std::string &table) {
		connection.BeginTransaction();
		try {
			RestBoundScan bound;
			auto &context = *connection.context;
			auto info = PaimonFunctions::GetPaimonScanFunction();
			vector<LogicalType> argument_types(3, LogicalType(LogicalTypeId::VARCHAR));
			bound.function = BoundTableFunction(info.functions.GetFunctionByArguments(context, argument_types));
			vector<Value> args {Value("branch-test"), Value("db"), Value(table)};
			// Attached tables supply these catalog options directly to the bind callback;
			// paimon_scan's SQL named parameters intentionally do not expose them.
			named_argument_map_t parameters;
			parameters["metastore"] = Value("rest");
			parameters["uri"] = Value(uri);
			parameters["token_provider"] = Value("bear");
			parameters["token"] = Value("test-token");
			vector<LogicalType> input_types;
			vector<Identifier> input_names;
			TableFunctionRef ref;
			TableFunctionBindInput input(args, parameters, input_types, input_names, nullptr, nullptr, bound.function,
			                             ref);
			bound.data = bound.function.bind(context, input, bound.types, bound.names);
			REQUIRE(bound.data);
			REQUIRE_FALSE(bound.data->SupportStatementCache());
			connection.Commit();
			return bound;
		} catch (...) {
			connection.Rollback();
			throw;
		}
	}

	void CheckEmptyRead(RestBoundScan &bound) {
		GetPartitionStatsInput stats_input(bound.function, *bound.data);
		auto stats = bound.function.get_partition_stats(*connection.context, stats_input);
		REQUIRE(stats.size() == 1);
		REQUIRE(stats[0].count_type == CountType::COUNT_EXACT);
		REQUIRE(stats[0].count == 0);
		vector<column_t> columns;
		for (idx_t i = 0; i < bound.types.size(); ++i) {
			columns.push_back(i);
		}
		TableFunctionInitInput input(bound.data.get(), columns, {}, nullptr);
		auto global = bound.function.init_global(*connection.context, input);
		ThreadContext thread(*connection.context);
		ExecutionContext execution(*connection.context, thread, nullptr);
		auto local = bound.function.init_local(execution, input, global.get());
		TableFunctionInput scan_input(bound.data.get(), local.get(), global.get());
		DataChunk output;
		output.Initialize(Allocator::Get(*connection.context), bound.types);
		bound.function.function(*connection.context, scan_input, output);
		REQUIRE(output.size() == 0);
	}

	void CheckRequests(const std::vector<std::string> &expected) {
		Stop();
		REQUIRE(listen_ok);
		std::lock_guard<std::mutex> lock(mutex);
		CAPTURE(unexpected, requests);
		REQUIRE(unexpected.empty());
		REQUIRE(requests == expected);
	}
};

void CheckEmptyBranch(const std::string &table, bool option_dev, bool dev) {
	RestBranchFixture fixture(option_dev, dev);
	auto bound = fixture.Bind(table);
	REQUIRE(bound.names.size() == 1);
	REQUIRE(bound.names[0] == (dev ? "dev_value" : "main_value"));
	REQUIRE(bound.types.size() == 1);
	REQUIRE(bound.types[0] == LogicalType(dev ? LogicalTypeId::VARCHAR : LogicalTypeId::INTEGER));
	fixture.CheckEmptyRead(bound);
	const auto selected = dev ? fixture.base + "$branch_dev" : fixture.base;
	std::vector<std::string> expected {"/v1/config"};
	if (option_dev && dev) {
		expected.push_back(fixture.base);
	}
	expected.insert(expected.end(), {selected, selected, selected + "/snapshots"});
	if (option_dev && dev) {
		expected.push_back(fixture.base);
	}
	expected.insert(expected.end(), {selected, selected});
	fixture.CheckRequests(expected);
}

TEST_CASE("Paimon REST branch: explicit main uses bare table URLs", "[paimon][rest_branch]") {
	CheckEmptyBranch("t$branch_main", true, false);
}

TEST_CASE("Paimon REST branch: table options route schema and snapshots to dev", "[paimon][rest_branch]") {
	CheckEmptyBranch("t", true, true);
}

TEST_CASE("Paimon REST branch: explicit dev uses branch schema and location", "[paimon][rest_branch]") {
	CheckEmptyBranch("t$branch_dev", false, true);
}

TEST_CASE("Paimon REST branch: snapshot HTTP failure is not empty success", "[paimon][rest_branch]") {
	RestBranchFixture fixture(false, true, true);
	REQUIRE_THROWS_WITH(fixture.Bind("t$branch_dev"), Catch::Contains("snapshot listing deliberately failed"));
	const auto selected = fixture.base + "$branch_dev";
	fixture.CheckRequests({"/v1/config", selected, selected, selected + "/snapshots"});
}

TEST_CASE("Paimon REST branch: missing read branch preserves attached physical entry", "[paimon][rest_branch]") {
	RestBranchFixture fixture(true, true, false, 404);
	fixture.Attach();
	fixture.CheckPhysicalEntry();
	fixture.CheckAttachedReadFailure();
	// The failed scan must not remove the cached physical catalog entry.
	fixture.CheckPhysicalEntry();
	const auto selected = fixture.base + "$branch_dev";
	fixture.CheckRequests(
	    {"/v1/config", "/v1/databases", fixture.base, selected, fixture.base, "/v1/config", fixture.base, selected});
}

TEST_CASE("Paimon REST branch: attached schema errors do not fall back to main", "[paimon][rest_branch]") {
	RestBranchFixture fixture(true, true, false, 400);
	fixture.Attach();
	REQUIRE_THROWS_WITH(fixture.CheckPhysicalEntry(), Catch::Contains("selected branch schema unavailable"));
	fixture.CheckAttachedReadFailure();
	const auto selected = fixture.base + "$branch_dev";
	fixture.CheckRequests({"/v1/config", "/v1/databases", fixture.base, selected, fixture.base, selected});
}

} // namespace
