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
#include "arrow/c/abi.h"
#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/read_context.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/plan.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

class InlineExecutor : public paimon::Executor {
public:
	void Add(std::function<void()> task) override {
		task();
	}
	void ShutdownNow() override {
	}
	uint32_t GetThreadNum() const override {
		return 1;
	}
};

// unique_ptr deletes the C structs, but does not invoke their release callbacks.
struct RootBatch {
	paimon::BatchReader::ReadBatch batch;
	explicit RootBatch(paimon::BatchReader::ReadBatch batch) : batch(std::move(batch)) {
	}
	~RootBatch() {
		ReleaseArray();
		ReleaseSchema();
	}
	void ReleaseArray() {
		if (batch.first && batch.first->release) {
			batch.first->release(batch.first.get());
		}
	}
	void ReleaseSchema() {
		if (batch.second && batch.second->release) {
			batch.second->release(batch.second.get());
		}
	}
};

struct CloseReader {
	void operator()(paimon::BatchReader *reader) const {
		if (reader) {
			reader->Close();
			delete reader;
		}
	}
};

struct NativeRead {
	std::shared_ptr<paimon::MemoryPool> pool;
	std::shared_ptr<paimon::Executor> executor = std::make_shared<InlineExecutor>();
	std::unique_ptr<paimon::TableScan> scan;
	std::shared_ptr<paimon::Plan> plan;
	std::unique_ptr<paimon::TableRead> table;
	std::unique_ptr<paimon::BatchReader, CloseReader> reader;

	explicit NativeRead(std::shared_ptr<paimon::MemoryPool> pool,
	                    const std::string &path = std::string(PAIMON_FIXTURE_DIR) + "/testdb.db/testtbl")
	    : pool(std::move(pool)) {
		const std::map<std::string, std::string> options {{"scan.mode", "from-snapshot-full"},
		                                                  {"scan.snapshot-id", "3"},
		                                                  {"read.batch-size", "2"},
		                                                  {"parquet.read.enable-pre-buffer", "false"},
		                                                  {"parquet.read.executor.thread-count", "0"}};
		paimon::ScanContextBuilder scan_builder(path);
		auto context = scan_builder.SetOptions(options).WithMemoryPool(this->pool).WithExecutor(executor).Finish();
		INFO(context.status().ToString());
		REQUIRE(context.ok());
		auto created_scan = paimon::TableScan::Create(std::move(context).value());
		INFO(created_scan.status().ToString());
		REQUIRE(created_scan.ok());
		scan = std::move(created_scan).value();
		auto planned = scan->CreatePlan();
		INFO(planned.status().ToString());
		REQUIRE(planned.ok());
		plan = std::move(planned).value();
		REQUIRE(plan->SnapshotId() == 3);
		REQUIRE_FALSE(plan->Splits().empty());

		paimon::ReadContextBuilder read_builder(path);
		auto read_context = read_builder.SetOptions(options)
		                        .SetReadFieldNames({"f0", "f1"})
		                        .WithMemoryPool(this->pool)
		                        .WithExecutor(executor)
		                        .EnablePrefetch(false)
		                        .EnableMultiThreadRowToBatch(false)
		                        .EnableLateMaterializing(false)
		                        .SetReadAheadCacheEnabled(false)
		                        .Finish();
		INFO(read_context.status().ToString());
		REQUIRE(read_context.ok());
		REQUIRE_FALSE(read_context.value()->EnablePrefetch());
		REQUIRE_FALSE(read_context.value()->EnableMultiThreadRowToBatch());
		REQUIRE_FALSE(read_context.value()->EnableLateMaterializing());
		REQUIRE_FALSE(read_context.value()->ReadAheadCacheEnabled());
		auto created_table = paimon::TableRead::Create(std::move(read_context).value());
		INFO(created_table.status().ToString());
		REQUIRE(created_table.ok());
		table = std::move(created_table).value();
	}

	void OpenReader() {
		auto created_reader = table->CreateReader(plan->Splits());
		INFO(created_reader.status().ToString());
		REQUIRE(created_reader.ok());
		reader.reset(std::move(created_reader).value().release());
	}
};

using Rows = std::vector<std::pair<std::string, int32_t>>;

Rows ReadRows(const RootBatch &root) {
	const auto &array = *root.batch.first;
	const auto &schema = *root.batch.second;
	REQUIRE(array.release);
	REQUIRE(schema.release);
	REQUIRE(std::string(schema.format) == "+s");
	REQUIRE(array.offset == 0);
	REQUIRE(array.null_count == 0);
	REQUIRE(array.length > 0);
	REQUIRE(array.length <= 2);
	REQUIRE(array.n_children == 3);
	REQUIRE(schema.n_children == 3);
	const std::vector<std::string> formats {"c", "u", "i"};
	const std::vector<std::string> names {"_VALUE_KIND", "f0", "f1"};
	for (int i = 0; i < 3; i++) {
		REQUIRE(std::string(schema.children[i]->format) == formats[i]);
		REQUIRE(std::string(schema.children[i]->name) == names[i]);
		REQUIRE(array.children[i]->offset == 0);
		REQUIRE(array.children[i]->length == array.length);
		REQUIRE(array.children[i]->null_count == 0);
	}
	const auto kinds = static_cast<const int8_t *>(array.children[0]->buffers[1]);
	const auto offsets = static_cast<const int32_t *>(array.children[1]->buffers[1]);
	const auto strings = static_cast<const char *>(array.children[1]->buffers[2]);
	const auto integers = static_cast<const int32_t *>(array.children[2]->buffers[1]);
	Rows rows;
	for (int64_t i = 0; i < array.length; i++) {
		REQUIRE(kinds[i] == 0);
		REQUIRE(offsets[i] >= 0);
		REQUIRE(offsets[i + 1] >= offsets[i]);
		rows.emplace_back(std::string(strings + offsets[i], offsets[i + 1] - offsets[i]), integers[i]);
	}
	return rows;
}

Rows ExpectedRows() {
	return {{"Alice", 1}, {"Bob", 1},   {"Cathy", 1}, {"David", 2}, {"Eve", 2},
	        {"Frank", 2}, {"Grace", 3}, {"Henry", 3}, {"Iris", 3}};
}

TEST_CASE("Paimon native roots survive subsequent reads and EOF", "[paimon][native_reader]") {
	const bool schema_first = GENERATE(false, true);
	auto pool = std::shared_ptr<paimon::MemoryPool>(paimon::GetMemoryPool());
	{
		NativeRead read(pool);
		read.OpenReader();
		std::vector<std::unique_ptr<RootBatch>> retained;
		Rows actual;
		while (true) {
			REQUIRE(retained.size() < 32);
			auto next = read.reader->NextBatch();
			INFO(next.status().ToString());
			REQUIRE(next.ok());
			auto root = std::make_unique<RootBatch>(std::move(next).value());
			if (paimon::BatchReader::IsEofBatch(root->batch)) {
				break;
			}
			REQUIRE(root->batch.first->length > 0);
			auto rows = ReadRows(*root);
			actual.insert(actual.end(), rows.begin(), rows.end());
			retained.push_back(std::move(root));
		}
		REQUIRE(retained.size() > 1);
		std::sort(actual.begin(), actual.end());
		REQUIRE(actual == ExpectedRows());
		Rows after_eof;
		for (const auto &root : retained) {
			auto rows = ReadRows(*root);
			after_eof.insert(after_eof.end(), rows.begin(), rows.end());
		}
		std::sort(after_eof.begin(), after_eof.end());
		REQUIRE(after_eof == ExpectedRows());
		// At native pin 53f9c86 the reader/adaptor must outlive all root releases.
		std::async(std::launch::async, [&]() {
			for (auto &root : retained) {
				if (schema_first) {
					root->ReleaseSchema();
					root->ReleaseArray();
				} else {
					root->ReleaseArray();
					root->ReleaseSchema();
				}
			}
		}).get();
		for (const auto &root : retained) {
			REQUIRE(root->batch.first->release == nullptr);
			REQUIRE(root->batch.second->release == nullptr);
		}
		std::async(std::launch::async, [&]() { read.reader.reset(); }).get();
	}
	REQUIRE(pool->MaxMemoryUsage() > 0);
	REQUIRE(pool->CurrentUsage() == 0);
}

TEST_CASE("Paimon native reader permits serialized worker handover", "[paimon][native_reader]") {
	const bool on_worker = GENERATE(false, true);
	auto pool = std::shared_ptr<paimon::MemoryPool>(paimon::GetMemoryPool());
	{
		NativeRead read(pool);
		read.OpenReader();
		Rows actual;
		size_t batches = 0;
		const auto creator = std::this_thread::get_id();
		while (true) {
			REQUIRE(batches++ < 32);
			std::thread::id caller;
			auto next_batch = [&]() {
				caller = std::this_thread::get_id();
				return read.reader->NextBatch();
			};
			// get() completes each call before another worker or release can proceed.
			auto next = on_worker ? std::async(std::launch::async, next_batch).get() : next_batch();
			REQUIRE((caller != creator) == on_worker);
			INFO(next.status().ToString());
			REQUIRE(next.ok());
			RootBatch root(std::move(next).value());
			if (paimon::BatchReader::IsEofBatch(root.batch)) {
				break;
			}
			auto rows = ReadRows(root);
			actual.insert(actual.end(), rows.begin(), rows.end());
		}
		REQUIRE(batches > 2);
		std::sort(actual.begin(), actual.end());
		REQUIRE(actual == ExpectedRows());
	}
	REQUIRE(pool->CurrentUsage() == 0);
}

TEST_CASE("Paimon EOF is not a zero-row Arrow root", "[paimon][native_reader]") {
	REQUIRE(paimon::BatchReader::IsEofBatch(paimon::BatchReader::MakeEofBatch()));
	RootBatch zero({std::make_unique<ArrowArray>(), std::make_unique<ArrowSchema>()});
	REQUIRE(zero.batch.first->length == 0);
	REQUIRE_FALSE(paimon::BatchReader::IsEofBatch(zero.batch));
}

struct FixtureCopy {
	std::filesystem::path path;
	FixtureCopy() {
		static std::atomic<uint64_t> next_id {0};
		const auto *root = std::getenv("PAIMON_TEST_ROOT");
		const auto base = root ? std::filesystem::path(root) : std::filesystem::temp_directory_path();
		const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
		path = base / ("native-reader-" + std::to_string(stamp) + "-" + std::to_string(next_id++));
		REQUIRE(std::filesystem::create_directories(path));
		std::filesystem::copy(std::string(PAIMON_FIXTURE_DIR) + "/testdb.db/testtbl", path,
		                      std::filesystem::copy_options::recursive);
	}
	~FixtureCopy() {
		std::error_code ignored;
		std::filesystem::remove_all(path, ignored);
	}
};

TEST_CASE("Paimon native missing data is an error, not EOF", "[paimon][native_reader]") {
	FixtureCopy fixture;
	auto pool = std::shared_ptr<paimon::MemoryPool>(paimon::GetMemoryPool());
	{
		NativeRead read(pool, fixture.path.string());
		std::vector<std::filesystem::path> files;
		for (const auto &entry : std::filesystem::recursive_directory_iterator(fixture.path)) {
			if (entry.path().extension() == ".parquet") {
				files.push_back(entry.path());
			}
		}
		REQUIRE_FALSE(files.empty());
		for (const auto &path : files) {
			REQUIRE(std::filesystem::remove(path));
		}
		// The selected plan still names the removed files. A lazy reader may fail
		// at creation or on its first read; neither may become successful EOF.
		auto opened = read.table->CreateReader(read.plan->Splits());
		paimon::Status error;
		if (!opened.ok()) {
			error = opened.status();
		} else {
			read.reader.reset(std::move(opened).value().release());
			auto next = read.reader->NextBatch();
			if (next.ok()) {
				RootBatch unexpected(std::move(next).value());
				FAIL("Missing data unexpectedly produced a successful batch or EOF");
			}
			error = next.status();
		}
		INFO(error.ToString());
		REQUIRE_FALSE(error.ok());
		REQUIRE(error.ToString().find(".parquet") != std::string::npos);
	}
	REQUIRE(pool->CurrentUsage() == 0);
}

} // namespace
