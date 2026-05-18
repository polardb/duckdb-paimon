/*-------------------------------------------------------------------------
 *
 * paimon_insert.cpp
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
 *	  src/paimon_storage/paimon_insert.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/main/client_context.hpp"

#include "paimon/commit_context.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/record_batch.h"
#include "paimon/write_context.h"

#include "paimon_insert.hpp"
#include "paimon_schema_entry.hpp"

#include <atomic>
#include <mutex>

namespace duckdb {

PhysicalPaimonInsert::PhysicalPaimonInsert(PhysicalPlan &physical_plan, LogicalOperator &op, SchemaCatalogEntry &schema,
                                           unique_ptr<BoundCreateTableInfo> info, string table_path,
                                           map<string, string> paimon_options, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, estimated_cardinality),
      schema(&schema), info(std::move(info)), table_path(std::move(table_path)),
      paimon_options(std::move(paimon_options)) {
}

// ---------------------------------------------------------------------------
// Sink state
// ---------------------------------------------------------------------------

struct PaimonInsertGlobalState : public GlobalSinkState {
	std::mutex lock;
	std::atomic<int32_t> next_write_id {0};
	std::vector<std::shared_ptr<paimon::CommitMessage>> all_commit_messages;
	idx_t insert_count = 0;
	bool finished = false;
};

struct PaimonInsertLocalState : public LocalSinkState {
	std::unique_ptr<paimon::FileStoreWrite> writer;
	idx_t local_count = 0;
};

// ---------------------------------------------------------------------------
// Sink interface
// ---------------------------------------------------------------------------

unique_ptr<GlobalSinkState> PhysicalPaimonInsert::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<PaimonInsertGlobalState>();

	if (info) {
		auto &paimon_schema = schema->Cast<PaimonSchemaEntry>();
		auto txn = CatalogTransaction::GetSystemCatalogTransaction(context);
		paimon_schema.CreateTable(txn, *info);
	}

	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalPaimonInsert::GetLocalSinkState(ExecutionContext &context) const {
	auto &gstate = sink_state->Cast<PaimonInsertGlobalState>();
	auto lstate = make_uniq<PaimonInsertLocalState>();

	int32_t write_id = gstate.next_write_id.fetch_add(1);

	paimon::WriteContextBuilder write_builder(table_path, "duckdb");
	auto write_ctx_result = write_builder.WithWriteId(write_id).SetOptions(paimon_options).Finish();
	if (!write_ctx_result.ok()) {
		throw IOException(write_ctx_result.status().ToString());
	}

	auto writer_result = paimon::FileStoreWrite::Create(std::move(write_ctx_result).value());
	if (!writer_result.ok()) {
		throw IOException(writer_result.status().ToString());
	}
	lstate->writer = std::move(writer_result).value();

	return std::move(lstate);
}

SinkResultType PhysicalPaimonInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<PaimonInsertLocalState>();

	ArrowArray out_array {};
	auto client_props = context.client.GetClientProperties();
	ArrowConverter::ToArrowArray(chunk, &out_array, client_props, {});
	struct ArrowArrayGuard {
		ArrowArray &array;
		~ArrowArrayGuard() {
			if (array.release) {
				array.release(&array);
			}
		}
	} arrow_guard {out_array};

	paimon::RecordBatchBuilder batch_builder(&out_array);
	auto batch_result = batch_builder.Finish();
	if (!batch_result.ok()) {
		throw IOException(batch_result.status().ToString());
	}

	auto status = lstate.writer->Write(std::move(batch_result).value());
	if (!status.ok()) {
		throw IOException(status.ToString());
	}

	lstate.local_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalPaimonInsert::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<PaimonInsertGlobalState>();
	auto &lstate = input.local_state.Cast<PaimonInsertLocalState>();

	if (lstate.local_count == 0) {
		auto close_status = lstate.writer->Close();
		if (!close_status.ok()) {
			throw IOException(close_status.ToString());
		}
		lstate.writer.reset();
		return SinkCombineResultType::FINISHED;
	}

	auto commit_msgs_result = lstate.writer->PrepareCommit();
	if (!commit_msgs_result.ok()) {
		throw IOException(commit_msgs_result.status().ToString());
	}
	auto local_messages = std::move(commit_msgs_result).value();

	auto close_status = lstate.writer->Close();
	if (!close_status.ok()) {
		throw IOException(close_status.ToString());
	}
	lstate.writer.reset();

	lock_guard<std::mutex> guard(gstate.lock);
	gstate.insert_count += lstate.local_count;
	for (auto &msg : local_messages) {
		gstate.all_commit_messages.push_back(std::move(msg));
	}

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalPaimonInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<PaimonInsertGlobalState>();

	if (gstate.all_commit_messages.empty()) {
		return SinkFinalizeType::READY;
	}

	paimon::CommitContextBuilder commit_builder(table_path, "duckdb");
	auto commit_ctx_result = commit_builder.SetOptions(paimon_options).Finish();
	if (!commit_ctx_result.ok()) {
		throw IOException(commit_ctx_result.status().ToString());
	}

	auto committer_result = paimon::FileStoreCommit::Create(std::move(commit_ctx_result).value());
	if (!committer_result.ok()) {
		throw IOException(committer_result.status().ToString());
	}

	auto commit_status = committer_result.value()->Commit(gstate.all_commit_messages);
	if (!commit_status.ok()) {
		throw IOException(commit_status.ToString());
	}

	return SinkFinalizeType::READY;
}

// ---------------------------------------------------------------------------
// Source interface (emits row count)
// ---------------------------------------------------------------------------

SourceResultType PhysicalPaimonInsert::GetData(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<PaimonInsertGlobalState>();
	if (gstate.finished) {
		return SourceResultType::FINISHED;
	}

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(gstate.insert_count));
	gstate.finished = true;

	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
