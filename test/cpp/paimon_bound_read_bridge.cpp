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
#include <thread>
#include "duckdb/common/arrow/arrow.hpp"
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
	BoundTableFunction function;
	unique_ptr<FunctionData> data;
};

struct BridgeFixture {
	DBConfig config;
	unique_ptr<DuckDB> db;
	unique_ptr<Connection> reader;
	unique_ptr<Connection> writer;
	string warehouse;
	TableFunction trusted;

	BridgeFixture() {
		config.SetOptionByName("allow_unsigned_extensions", Value(true));
		config.SetOptionByName("autoinstall_known_extensions", Value(false));
		config.SetOptionByName("autoload_known_extensions", Value(false));
		db = make_uniq<DuckDB>(nullptr, &config);
		reader = make_uniq<Connection>(*db);
		writer = make_uniq<Connection>(*db);
#if !PAIMON_BRIDGE_BUILTIN
		SQL(*reader, "LOAD " + Value(PAIMON_BRIDGE_EXTENSION_PATH).ToSQLString());
#endif
		reader->BeginTransaction();
		auto &entry = Catalog::GetEntry<TableFunctionCatalogEntry>(
		    *reader->context, QualifiedName(SYSTEM_CATALOG, DEFAULT_SCHEMA, "paimon_scan"));
		trusted = *entry.functions.GetFunctionByOffset(0);
		reader->Commit();
#if !PAIMON_BRIDGE_BUILTIN && (defined(__unix__) || defined(__APPLE__))
		CheckOrigin(reinterpret_cast<const void *>(trusted.function));
		CheckOrigin(reinterpret_cast<const void *>(trusted.bind));
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
				auto &catalog_entry =
				    Catalog::GetEntry<TableCatalogEntry>(context, QualifiedName("pm", "race", Identifier(table)));
				bound.function = BoundTableFunction(catalog_entry.GetScanFunction(context, bound.data));
			} else {
				auto &catalog_entry = Catalog::GetEntry<TableFunctionCatalogEntry>(
				    context, QualifiedName(SYSTEM_CATALOG, DEFAULT_SCHEMA, "paimon_scan"));
				auto argument_count = entry == 0 ? 1 : 3;
				vector<LogicalType> argument_types(argument_count, LogicalType::VARCHAR);
				bound.function =
				    BoundTableFunction(catalog_entry.functions.GetFunctionByArguments(context, argument_types));
				REQUIRE(bound.function.bind);
				vector<Value> args = entry == 0 ? vector<Value> {Value(warehouse + "/race.db/" + table)}
				                                : vector<Value> {Value(warehouse), Value("race"), Value(table)};
				named_argument_map_t parameters;
				bound.function.SetCallArguments(args, parameters);
				vector<LogicalType> input_types, return_types;
				vector<Identifier> input_names, names;
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

#if !PAIMON_BRIDGE_BUILTIN && (defined(__unix__) || defined(__APPLE__))
	static void CheckOrigin(const void *callback) {
		Dl_info origin {};
		REQUIRE(dladdr(callback, &origin) != 0);
		REQUIRE(origin.dli_fname);
		REQUIRE(std::filesystem::equivalent(origin.dli_fname, PAIMON_BRIDGE_EXTENSION_PATH));
	}
#endif

	const PaimonReadBridgeV1 &API(const BridgeBinding &bound) {
		// Fixture controls LOAD and captures the definition before any catalog edits.
		auto *api = TryGetPaimonReadBridge(bound.function, trusted);
		REQUIRE(api);
#if !PAIMON_BRIDGE_BUILTIN && (defined(__unix__) || defined(__APPLE__))
		CheckOrigin(reinterpret_cast<const void *>(api->accepts_bind));
		CheckOrigin(reinterpret_cast<const void *>(api->retain));
		CheckOrigin(reinterpret_cast<const void *>(api->describe));
		CheckOrigin(reinterpret_cast<const void *>(api->release));
#endif
		return *api;
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

TEST_CASE_METHOD(BridgeFixture, "Paimon bridge: discovery rejects foreign definitions before wrapper access",
                 "[paimon][bound_read_bridge]") {
	auto bound = Bind();
	REQUIRE(TryGetPaimonReadBridge(bound.function, trusted));
	auto candidate = bound.function;
	SECTION("foreign object with matching callbacks") {
		candidate.function_info = make_shared_ptr<TableFunctionInfo>();
	}
	SECTION("missing wrapper") {
		candidate.function_info.reset();
	}
	SECTION("wrong scan callback") {
		candidate.function = nullptr;
	}
	SECTION("wrong bind callback") {
		candidate.bind = nullptr;
	}
	SECTION("wrong global callback") {
		candidate.init_global = nullptr;
	}
	SECTION("wrong local callback") {
		candidate.init_local = nullptr;
	}
	SECTION("foreign scan with genuine wrapper") {
		reader->BeginTransaction();
		auto &foreign = Catalog::GetEntry<TableFunctionCatalogEntry>(
		    *reader->context, QualifiedName(SYSTEM_CATALOG, DEFAULT_SCHEMA, "repeat"));
		candidate = BoundTableFunction(foreign.functions.GetFunctionByOffset(0));
		reader->Commit();
		REQUIRE(candidate.function);
		REQUIRE(candidate.function != trusted.function);
		candidate.function_info = trusted.function_info;
	}
	REQUIRE_FALSE(TryGetPaimonReadBridge(candidate, trusted));
	REQUIRE_FALSE(TryGetPaimonReadBridge(bound.function, TableFunction()));
}

TEST_CASE_METHOD(BridgeFixture, "Paimon bridge: discovery validates authenticated API", "[paimon][bound_read_bridge]") {
	auto bound = Bind();
	auto api = API(bound);
	SECTION("wrong magic") {
		api.magic++;
	}
	SECTION("wrong version") {
		api.version++;
	}
	SECTION("wrong size") {
		api.struct_size--;
	}
	SECTION("missing accepts_bind") {
		api.accepts_bind = nullptr;
	}
	SECTION("missing retain") {
		api.retain = nullptr;
	}
	SECTION("missing describe") {
		api.describe = nullptr;
	}
	SECTION("missing release") {
		api.release = nullptr;
	}
	// This locally constructed definition is known to have the wrapper type.
	auto definition = trusted;
	definition.function_info = make_shared_ptr<PaimonReadBridgeInfo>(api);
	REQUIRE_FALSE(TryGetPaimonReadBridge(definition, definition));
}

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
	config.SetOptionByName("autoinstall_known_extensions", Value(false));
	config.SetOptionByName("autoload_known_extensions", Value(false));
	DuckDB db(nullptr, &config);
	Connection con(db);
	auto catalog_absent = [&]() {
		auto result = con.Query("SELECT count(*) FROM duckdb_functions() WHERE function_name='paimon_scan'");
		REQUIRE_FALSE(result->HasError());
		auto chunk = result->Fetch();
		REQUIRE(chunk);
		REQUIRE(chunk->size() == 1);
		REQUIRE(chunk->GetValue(0, 0).GetValue<int64_t>() == 0);
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

static_assert(std::is_standard_layout<PaimonReadBridgeV2>::value, "Native bridge must have C layout");
using NativeHandle = std::unique_ptr<void, void (*)(void *)>;

static const PaimonReadBridgeV2 &NativeAPI(BridgeFixture &fixture, const BridgeBinding &bound) {
	auto *api = TryGetPaimonNativeReadBridge(bound.function, fixture.trusted);
	REQUIRE(api);
#if !PAIMON_BRIDGE_BUILTIN && (defined(__unix__) || defined(__APPLE__))
	BridgeFixture::CheckOrigin(reinterpret_cast<const void *>(api->create_plan));
	BridgeFixture::CheckOrigin(reinterpret_cast<const void *>(api->next_batch));
	BridgeFixture::CheckOrigin(reinterpret_cast<const void *>(api->release_batch));
#endif
	return *api;
}

static NativeHandle Plan(const PaimonReadBridgeV2 &api, Descriptor &descriptor, uint64_t &count,
                         uint64_t byte_limit = UINT64_MAX) {
	PaimonPlanOptionsV1 options {sizeof(options), byte_limit};
	void *raw = nullptr;
	char error[128];
	auto status = api.create_plan(descriptor.handle, &options, nullptr, &raw, &count, error, sizeof(error));
	NativeHandle result(raw, api.release_plan);
	INFO(error);
	REQUIRE(status == PAIMON_READ_OK);
	REQUIRE(result);
	return result;
}

static NativeHandle Split(const PaimonReadBridgeV2 &api, const NativeHandle &plan, uint64_t index) {
	void *raw = nullptr;
	auto status = api.get_split(plan.get(), index, &raw);
	NativeHandle result(raw, api.release_split);
	REQUIRE(status == PAIMON_READ_OK);
	REQUIRE(result);
	return result;
}

static NativeHandle Reader(const PaimonReadBridgeV2 &api, const NativeHandle &split) {
	void *raw = nullptr;
	char error[128];
	auto status = api.open_reader(split.get(), nullptr, &raw, error, sizeof(error));
	NativeHandle result(raw, api.release_reader);
	INFO(error);
	REQUIRE(status == PAIMON_READ_OK);
	REQUIRE(result);
	return result;
}

static void CheckIntegerRoot(const ArrowArray &array, const ArrowSchema &schema, std::map<int32_t, int32_t> &rows) {
	REQUIRE(array.release);
	REQUIRE(schema.release);
	REQUIRE(string(schema.format) == "+s");
	REQUIRE(array.n_children == 3);
	REQUIRE(schema.n_children == 3);
	REQUIRE(string(schema.children[1]->name) == "id");
	REQUIRE(string(schema.children[2]->name) == "value");
	REQUIRE(string(schema.children[1]->format) == "i");
	REQUIRE(string(schema.children[2]->format) == "i");
	REQUIRE(array.length >= 0);
	for (int col = 1; col <= 2; col++) {
		REQUIRE(array.children[col]);
		REQUIRE(array.children[col]->null_count == 0);
		REQUIRE(array.children[col]->n_buffers == 2);
		REQUIRE(array.children[col]->buffers[1]);
	}
	for (int64_t i = 0; i < array.length; i++) {
		auto id =
		    static_cast<const int32_t *>(array.children[1]->buffers[1])[i + array.offset + array.children[1]->offset];
		auto value =
		    static_cast<const int32_t *>(array.children[2]->buffers[1])[i + array.offset + array.children[2]->offset];
		REQUIRE(rows.emplace(id, value).second);
	}
}

TEST_CASE_METHOD(BridgeFixture, "Paimon native bridge: retained snapshot reads across DSO and owner release",
                 "[paimon][bound_read_bridge][native_read_bridge]") {
	auto entry = GENERATE(0, 1, 2);
	auto format = GENERATE("parquet", "orc");
#if defined(__linux__)
	auto byte_limit = GENERATE(UINT64_MAX, uint64_t(64 * 1024 * 1024), UINT64_MAX - uint64_t(1));
#else
	// The native SDK explicitly refuses finite planning on other platforms.
	auto byte_limit = UINT64_MAX;
#endif
	SQL(*writer, "CREATE TABLE pm.race.payload (id INTEGER, value INTEGER) WITH ('file.format'='" + string(format) +
	                 "', 'bucket'='-1', 'parquet.read.executor.thread-count'='invalid', "
	                 "'parquet.read.enable-pre-buffer'='invalid')");
	SQL(*writer, "INSERT INTO pm.race.payload SELECT i::INTEGER, (i * 7)::INTEGER FROM range(5000) r(i)");
	auto bound = Bind(entry, "payload");
	auto &api = NativeAPI(*this, bound);
	Descriptor descriptor(api.descriptor);
	Retain(bound, descriptor);
	bound.data.reset();
	SQL(*writer, "INSERT INTO pm.race.payload VALUES (9000, -1)");
	uint64_t count = 0;
	auto plan = Plan(api, descriptor, count, byte_limit);
	REQUIRE(count > 0);
	api.descriptor.release(descriptor.handle);
	descriptor.handle = nullptr;
	std::map<int32_t, int32_t> rows;
	for (uint64_t i = 0; i < count; i++) {
		auto split = Split(api, plan, i);
		auto native_reader = Reader(api, split);
		split.reset();
		for (;;) {
			void *raw = nullptr;
			const ArrowArray *array = nullptr;
			const ArrowSchema *schema = nullptr;
			auto status = api.next_batch(native_reader.get(), nullptr, &raw, &array, &schema, nullptr, 0);
			NativeHandle batch(raw, api.release_batch);
			if (status == PAIMON_READ_EOF) {
				REQUIRE_FALSE(batch);
				REQUIRE_FALSE(array);
				REQUIRE_FALSE(schema);
				REQUIRE(api.next_batch(native_reader.get(), nullptr, &raw, &array, &schema, nullptr, 0) ==
				        PAIMON_READ_EOF);
				break;
			}
			REQUIRE(status == PAIMON_READ_OK);
			REQUIRE(batch);
			REQUIRE(array);
			REQUIRE(schema);
			// A second request cannot invalidate or overwrite this root.
			void *refused = reinterpret_cast<void *>(1);
			const ArrowArray *refused_array = array;
			const ArrowSchema *refused_schema = schema;
			REQUIRE(api.next_batch(native_reader.get(), nullptr, &refused, &refused_array, &refused_schema, nullptr,
			                       0) == PAIMON_READ_RESOURCE_REFUSED);
			REQUIRE_FALSE(refused);
			REQUIRE_FALSE(refused_array);
			REQUIRE_FALSE(refused_schema);
			CheckIntegerRoot(*array, *schema, rows);
		}
	}
	REQUIRE(rows.size() == 5000);
	for (int32_t id = 0; id < 5000; id++) {
		REQUIRE(rows.at(id) == id * 7);
	}
}

TEST_CASE_METHOD(BridgeFixture, "Paimon native bridge: batch retains reader and plan until complete-root release",
                 "[paimon][bound_read_bridge][native_read_bridge]") {
	SQL(*writer, "INSERT INTO pm.race.t VALUES (1, 7), (2, 14)");
	auto bound = Bind();
	auto &api = NativeAPI(*this, bound);
	Descriptor descriptor(api.descriptor);
	Retain(bound, descriptor);
	uint64_t count;
	auto plan = Plan(api, descriptor, count);
	REQUIRE(count == 1);
	auto split = Split(api, plan, 0);
	auto native_reader = Reader(api, split);
	void *raw = nullptr;
	const ArrowArray *array = nullptr;
	const ArrowSchema *schema = nullptr;
	int32_t status = -1;
	// Serialized worker handover, with no assertion library calls on the worker.
	std::thread worker(
	    [&]() { status = api.next_batch(native_reader.get(), nullptr, &raw, &array, &schema, nullptr, 0); });
	worker.join();
	NativeHandle batch(raw, api.release_batch);
	REQUIRE(status == PAIMON_READ_OK);
	REQUIRE(batch);
	native_reader.reset();
	split.reset();
	plan.reset();
	api.descriptor.release(descriptor.handle);
	descriptor.handle = nullptr;
	bound.data.reset();
	std::map<int32_t, int32_t> rows;
	CheckIntegerRoot(*array, *schema, rows);
	REQUIRE((rows == std::map<int32_t, int32_t> {{1, 7}, {2, 14}}));
	batch.reset();
	SQL(*writer, "SELECT 42");
}

TEST_CASE_METHOD(BridgeFixture, "Paimon native bridge: empty at bind never reads a later snapshot",
                 "[paimon][bound_read_bridge][native_read_bridge]") {
	auto bound = Bind(GENERATE(0, 1, 2));
	auto &api = NativeAPI(*this, bound);
	Descriptor descriptor(api.descriptor);
	Retain(bound, descriptor);
	SQL(*writer, "INSERT INTO pm.race.t VALUES (1, 7)");
	uint64_t count = 99;
	auto plan = Plan(api, descriptor, count, GENERATE(uint64_t(0), UINT64_MAX));
	REQUIRE(count == 0);
	void *split = reinterpret_cast<void *>(1);
	REQUIRE(api.get_split(plan.get(), 0, &split) == PAIMON_READ_INVALID_ARGUMENT);
	REQUIRE_FALSE(split);
}

struct CancelAfterChecks {
	int checks = 0;
	int cancel_at;
	static int32_t Check(const void *ptr) {
		auto &state = *const_cast<CancelAfterChecks *>(static_cast<const CancelAfterChecks *>(ptr));
		return ++state.checks >= state.cancel_at;
	}
};

TEST_CASE_METHOD(BridgeFixture, "Paimon native bridge: exhausted budgets publish no plan and clear failed outputs",
                 "[paimon][bound_read_bridge][native_read_bridge]") {
	SQL(*writer, "INSERT INTO pm.race.t VALUES (1, 7)");
	auto bound = Bind();
	auto &api = NativeAPI(*this, bound);
	Descriptor descriptor(api.descriptor);
	Retain(bound, descriptor);
	std::filesystem::remove_all(warehouse);
	PaimonPlanOptionsV1 options {sizeof(options), 0};
	void *plan = reinterpret_cast<void *>(1);
	uint64_t count = 99;
	char error[128];
	for (auto limit : {uint64_t(0), uint64_t(1)}) {
		options.planning_byte_limit = limit;
		plan = reinterpret_cast<void *>(1);
		count = 99;
#if defined(__linux__)
		REQUIRE(api.create_plan(descriptor.handle, &options, nullptr, &plan, &count, error, sizeof(error)) ==
		        PAIMON_READ_RESOURCE_REFUSED);
		REQUIRE(string(error).find("budget") != string::npos);
#else
		REQUIRE(api.create_plan(descriptor.handle, &options, nullptr, &plan, &count, error, sizeof(error)) ==
		        PAIMON_READ_UNSUPPORTED);
#endif
		REQUIRE_FALSE(plan);
		REQUIRE(count == 0);
	}
	options.planning_byte_limit = UINT64_MAX;
	REQUIRE(api.create_plan(descriptor.handle, &options, nullptr, &plan, &count, error, sizeof(error)) ==
	        PAIMON_READ_INTERNAL_ERROR);
	REQUIRE_FALSE(plan);
	REQUIRE(count == 0);
	REQUIRE(api.create_plan(nullptr, &options, nullptr, &plan, &count, error, sizeof(error)) ==
	        PAIMON_READ_INVALID_ARGUMENT);
	options.struct_size--;
	REQUIRE(api.create_plan(descriptor.handle, &options, nullptr, &plan, &count, error, sizeof(error)) ==
	        PAIMON_READ_INVALID_ARGUMENT);
	REQUIRE_FALSE(plan);
	REQUIRE(count == 0);
	api.release_plan(nullptr);
	api.release_split(nullptr);
	api.release_reader(nullptr);
	api.release_batch(nullptr);
}

TEST_CASE_METHOD(BridgeFixture, "Paimon native bridge: finite planning refuses unsupported profile without fallback",
                 "[paimon][bound_read_bridge][native_read_bridge]") {
	SQL(*writer, "CREATE TABLE pm.race.bucketed (id INTEGER, value INTEGER) WITH ('bucket'='1')");
	SQL(*writer, "INSERT INTO pm.race.bucketed VALUES (1, 7)");
	auto bound = Bind(0, "bucketed");
	auto &api = NativeAPI(*this, bound);
	Descriptor descriptor(api.descriptor);
	Retain(bound, descriptor);
	PaimonPlanOptionsV1 options {sizeof(options), 64 * 1024 * 1024};
	void *plan = reinterpret_cast<void *>(1);
	uint64_t count = 99;
	char error[128];
	REQUIRE(api.create_plan(descriptor.handle, &options, nullptr, &plan, &count, error, sizeof(error)) ==
	        PAIMON_READ_UNSUPPORTED);
	REQUIRE_FALSE(plan);
	REQUIRE(count == 0);
	REQUIRE(error[0]);
}

TEST_CASE_METHOD(BridgeFixture, "Paimon native bridge: missing retained snapshots and payloads are errors not EOF",
                 "[paimon][bound_read_bridge][native_read_bridge]") {
#if defined(__linux__)
	auto byte_limit = GENERATE(UINT64_MAX, uint64_t(64 * 1024 * 1024));
#else
	auto byte_limit = UINT64_MAX;
#endif
	SQL(*writer, "CREATE TABLE pm.race.failures (id INTEGER, value INTEGER) WITH ('bucket'='-1')");
	SQL(*writer, "INSERT INTO pm.race.failures VALUES (1, 7)");
	auto bound = Bind(0, "failures");
	auto &api = NativeAPI(*this, bound);
	Descriptor descriptor(api.descriptor);
	Retain(bound, descriptor);
	SECTION("retained snapshot expired despite newer snapshot") {
		SQL(*writer, "INSERT INTO pm.race.failures VALUES (2, 14)");
		REQUIRE(std::filesystem::remove(warehouse + "/race.db/failures/snapshot/snapshot-1"));
		uint64_t count = 9;
		void *raw = nullptr;
		PaimonPlanOptionsV1 options {sizeof(options), byte_limit};
		REQUIRE(api.create_plan(descriptor.handle, &options, nullptr, &raw, &count, nullptr, 0) ==
		        PAIMON_READ_INTERNAL_ERROR);
		REQUIRE_FALSE(raw);
		REQUIRE(count == 0);
	}
	SECTION("data removed after plan") {
		uint64_t count;
		auto plan = Plan(api, descriptor, count, byte_limit);
		REQUIRE(count == 1);
		auto split = Split(api, plan, 0);
		uint64_t removed = 0;
		for (auto &file : std::filesystem::recursive_directory_iterator(warehouse)) {
			if (file.path().extension() == ".parquet") {
				removed += std::filesystem::remove(file.path());
			}
		}
		REQUIRE(removed > 0);
		void *raw = nullptr;
		auto status = api.open_reader(split.get(), nullptr, &raw, nullptr, 0);
		NativeHandle native_reader(raw, api.release_reader);
		if (status == PAIMON_READ_OK) {
			const ArrowArray *array = nullptr;
			const ArrowSchema *schema = nullptr;
			status = api.next_batch(native_reader.get(), nullptr, &raw, &array, &schema, nullptr, 0);
			REQUIRE_FALSE(raw);
			REQUIRE_FALSE(array);
			REQUIRE_FALSE(schema);
			REQUIRE(api.next_batch(native_reader.get(), nullptr, &raw, &array, &schema, nullptr, 0) == status);
		}
		REQUIRE(status == PAIMON_READ_INTERNAL_ERROR);
	}
}

TEST_CASE_METHOD(BridgeFixture, "Paimon native bridge: cancellation before and after native calls publishes nothing",
                 "[paimon][bound_read_bridge][native_read_bridge]") {
	SQL(*writer, "INSERT INTO pm.race.t VALUES (1, 7)");
	auto bound = Bind();
	auto &api = NativeAPI(*this, bound);
	Descriptor descriptor(api.descriptor);
	Retain(bound, descriptor);
	auto boundary = GENERATE(1, 2);
	CancelAfterChecks state {0, boundary};
	PaimonReadCancellationV1 cancel {&state, CancelAfterChecks::Check};
	SECTION("planning") {
		uint64_t count = 99;
		void *raw = nullptr;
		PaimonPlanOptionsV1 options {sizeof(options), UINT64_MAX};
		REQUIRE(api.create_plan(descriptor.handle, &options, &cancel, &raw, &count, nullptr, 0) ==
		        PAIMON_READ_CANCELLED);
		REQUIRE_FALSE(raw);
		REQUIRE(count == 0);
	}
	SECTION("reader creation") {
		uint64_t count;
		auto plan = Plan(api, descriptor, count);
		auto split = Split(api, plan, 0);
		void *raw = nullptr;
		REQUIRE(api.open_reader(split.get(), &cancel, &raw, nullptr, 0) == PAIMON_READ_CANCELLED);
		REQUIRE_FALSE(raw);
	}
	SECTION("batch creation, including successful native root discarded on cancellation") {
		uint64_t count;
		auto plan = Plan(api, descriptor, count);
		auto split = Split(api, plan, 0);
		auto native_reader = Reader(api, split);
		void *raw = nullptr;
		const ArrowArray *array = nullptr;
		const ArrowSchema *schema = nullptr;
		REQUIRE(api.next_batch(native_reader.get(), &cancel, &raw, &array, &schema, nullptr, 0) ==
		        PAIMON_READ_CANCELLED);
		REQUIRE_FALSE(raw);
		REQUIRE_FALSE(array);
		REQUIRE_FALSE(schema);
		REQUIRE(api.next_batch(native_reader.get(), nullptr, &raw, &array, &schema, nullptr, 0) ==
		        PAIMON_READ_CANCELLED);
	}
}

TEST_CASE_METHOD(BridgeFixture, "Paimon native bridge: exact V1 remains descriptor-only and malformed V2 is refused",
                 "[paimon][bound_read_bridge][native_read_bridge]") {
	auto bound = Bind();
	auto native = NativeAPI(*this, bound);
	SECTION("synthetic authenticated V1 provider") {
		auto prefix = native.descriptor;
		prefix.version = PAIMON_READ_BRIDGE_VERSION;
		prefix.struct_size = sizeof(prefix);
		auto definition = trusted;
		definition.function_info = make_shared_ptr<PaimonReadBridgeInfo>(prefix);
		REQUIRE(TryGetPaimonReadBridge(definition, definition) == &prefix);
		REQUIRE_FALSE(TryGetPaimonNativeReadBridge(definition, definition));
		Descriptor descriptor(prefix);
		Retain(bound, descriptor);
		REQUIRE(descriptor.View().has_snapshot == 0);
		return;
	}
	SECTION("missing planning") {
		native.create_plan = nullptr;
	}
	SECTION("missing plan release") {
		native.release_plan = nullptr;
	}
	SECTION("missing split access") {
		native.get_split = nullptr;
	}
	SECTION("missing split release") {
		native.release_split = nullptr;
	}
	SECTION("missing reader creation") {
		native.open_reader = nullptr;
	}
	SECTION("missing reader release") {
		native.release_reader = nullptr;
	}
	SECTION("missing batch read") {
		native.next_batch = nullptr;
	}
	SECTION("missing batch release") {
		native.release_batch = nullptr;
	}
	auto definition = trusted;
	definition.function_info = make_shared_ptr<PaimonReadBridgeInfo>(native.descriptor);
	REQUIRE_FALSE(TryGetPaimonNativeReadBridge(definition, definition));
	REQUIRE_FALSE(TryGetPaimonNativeReadBridge(TableFunction(), trusted));
}

TEST_CASE_METHOD(BridgeFixture, "Paimon native bridge: validates handle outputs and refuses unqualified reader kinds",
                 "[paimon][bound_read_bridge][native_read_bridge]") {
	auto bound = Bind();
	auto &api = NativeAPI(*this, bound);
	void *raw = reinterpret_cast<void *>(1);
	const ArrowArray *array = reinterpret_cast<const ArrowArray *>(1);
	const ArrowSchema *schema = reinterpret_cast<const ArrowSchema *>(1);
	REQUIRE(api.get_split(nullptr, 0, &raw) == PAIMON_READ_INVALID_ARGUMENT);
	REQUIRE_FALSE(raw);
	REQUIRE(api.open_reader(nullptr, nullptr, &raw, nullptr, 0) == PAIMON_READ_INVALID_ARGUMENT);
	REQUIRE_FALSE(raw);
	REQUIRE(api.next_batch(nullptr, nullptr, &raw, &array, &schema, nullptr, 0) == PAIMON_READ_INVALID_ARGUMENT);
	REQUIRE_FALSE(raw);
	REQUIRE_FALSE(array);
	REQUIRE_FALSE(schema);
	auto table = GENERATE("pk", "evolved");
	if (string(table) == "pk") {
		SQL(*writer, "CREATE TABLE pm.race.pk (id INTEGER PRIMARY KEY) WITH ('bucket'='1')");
	} else {
		SQL(*writer, "CREATE TABLE pm.race.evolved (id INTEGER) WITH ('data-evolution.enabled'='true', "
		             "'row-tracking.enabled'='true')");
	}
	auto unsupported = Bind(0, table);
	Descriptor descriptor(api.descriptor);
	Retain(unsupported, descriptor);
	uint64_t count = 99;
	PaimonPlanOptionsV1 options {sizeof(options), UINT64_MAX};
	REQUIRE(api.create_plan(descriptor.handle, &options, nullptr, &raw, &count, nullptr, 0) == PAIMON_READ_UNSUPPORTED);
	REQUIRE_FALSE(raw);
	REQUIRE(count == 0);
}
