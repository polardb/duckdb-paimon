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

#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/commit_context.h"
#include "paimon/defs.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/record_batch.h"
#include "paimon/schema/schema.h"
#include "paimon/write_context.h"

#include "paimon_catalog.hpp"
#include "paimon_insert.hpp"
#include "paimon_schema_entry.hpp"

#include <atomic>
#include <mutex>

namespace duckdb {

PhysicalPaimonInsert::PhysicalPaimonInsert(PhysicalPlan &physical_plan, LogicalOperator &op, SchemaCatalogEntry &schema,
                                           unique_ptr<BoundCreateTableInfo> info, string table_path,
                                           map<string, string> paimon_options, vector<string> partition_keys,
                                           idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, estimated_cardinality),
      schema(&schema), info(std::move(info)), table_path(std::move(table_path)),
      paimon_options(std::move(paimon_options)), partition_keys(std::move(partition_keys)) {
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

	vector<string> part_key_names;
	vector<idx_t> part_col_idxs;
	string null_part_name = "__DEFAULT_PARTITION__";
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

	if (!partition_keys.empty()) {
		auto &paimon_catalog = schema->catalog.Cast<PaimonCatalog>();
		auto &catalog = paimon_catalog.GetPaimonCatalog();
		auto schema_name = info ? info->Base().schema : schema->name;
		auto table_name = info ? info->Base().table : string();

		if (table_name.empty()) {
			auto last_slash = table_path.rfind('/');
			table_name = (last_slash != string::npos) ? table_path.substr(last_slash + 1) : table_path;
		}

		auto schema_result = catalog.LoadTableSchema(paimon::Identifier(schema_name, table_name));
		if (schema_result.ok()) {
			auto table_schema = schema_result.value();
			auto data_schema = std::dynamic_pointer_cast<paimon::DataSchema>(table_schema);
			if (data_schema) {
				auto &table_options = data_schema->Options();
				auto default_name = table_options.find(paimon::Options::PARTITION_DEFAULT_NAME);
				if (default_name != table_options.end()) {
					state->null_part_name = default_name->second;
				}
			}

			auto field_names = table_schema->FieldNames();
			state->part_key_names = partition_keys;
			for (auto &part_key : partition_keys) {
				auto it = std::find(field_names.begin(), field_names.end(), part_key);
				if (it != field_names.end()) {
					state->part_col_idxs.push_back(std::distance(field_names.begin(), it));
				}
			}
		}
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

static void WriteChunkWithPartition(PaimonInsertLocalState &lstate, DataChunk &chunk, ClientContext &client,
                                    const std::map<string, string> &partition) {
	ArrowArrayWrapper arrow_wrapper;
	auto client_props = client.GetClientProperties();
	ArrowConverter::ToArrowArray(chunk, &arrow_wrapper.arrow_array, client_props, {});

	paimon::RecordBatchBuilder batch_builder(&arrow_wrapper.arrow_array);
	if (!partition.empty()) {
		batch_builder.SetPartition(partition).SetBucket(0);
	}
	auto batch_result = batch_builder.Finish();
	if (!batch_result.ok()) {
		throw IOException(batch_result.status().ToString());
	}

	auto status = lstate.writer->Write(std::move(batch_result).value());
	if (!status.ok()) {
		throw IOException(status.ToString());
	}
}

SinkResultType PhysicalPaimonInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<PaimonInsertGlobalState>();
	auto &lstate = input.local_state.Cast<PaimonInsertLocalState>();

	if (gstate.part_col_idxs.empty()) {
		WriteChunkWithPartition(lstate, chunk, context.client, {});
		lstate.local_count += chunk.size();
		return SinkResultType::NEED_MORE_INPUT;
	}

	auto num_rows = chunk.size();
	auto &part_idxs = gstate.part_col_idxs;
	auto &part_names = gstate.part_key_names;
	D_ASSERT(part_idxs.size() == part_names.size());

	vector<UnifiedVectorFormat> part_formats(part_idxs.size());
	for (idx_t i = 0; i < part_idxs.size(); i++) {
		chunk.data[part_idxs[i]].ToUnifiedFormat(num_rows, part_formats[i]);
	}

	std::map<std::vector<string>, vector<idx_t>> partition_groups;
	for (idx_t row = 0; row < num_rows; row++) {
		std::vector<string> key;
		key.reserve(part_idxs.size());
		for (idx_t i = 0; i < part_idxs.size(); i++) {
			auto idx = part_formats[i].sel->get_index(row);
			if (!part_formats[i].validity.RowIsValid(idx)) {
				key.push_back(gstate.null_part_name);
			} else {
				key.push_back(chunk.GetValue(part_idxs[i], row).ToString());
			}
		}
		partition_groups[key].push_back(row);
	}

	for (auto &entry : partition_groups) {
		auto &key_values = entry.first;
		auto &row_indices = entry.second;

		std::map<string, string> partition;
		for (idx_t i = 0; i < part_names.size(); i++) {
			partition[part_names[i]] = key_values[i];
		}

		SelectionVector sel(row_indices.size());
		for (idx_t i = 0; i < row_indices.size(); i++) {
			sel.set_index(i, row_indices[i]);
		}

		DataChunk sub_chunk;
		sub_chunk.Initialize(Allocator::DefaultAllocator(), chunk.GetTypes());
		sub_chunk.Slice(chunk, sel, row_indices.size());

		WriteChunkWithPartition(lstate, sub_chunk, context.client, partition);
	}

	lstate.local_count += num_rows;
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
