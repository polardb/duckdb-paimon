/*-------------------------------------------------------------------------
 *
 * paimon_snapshots.cpp
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
 *      src/paimon_storage/paimon_snapshots.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "duckdb.hpp"

#include "paimon_catalog.hpp"
#include "paimon_functions.hpp"

#include "paimon/catalog/identifier.h"
#include "paimon/snapshot/snapshot_info.h"

namespace duckdb {

struct PaimonSnapshotsBindData : public TableFunctionData {
	PaimonTablePath path;
	unordered_map<string, Value> input_options;
};

struct PaimonSnapshotsGlobalState : public GlobalTableFunctionState {
	vector<paimon::SnapshotInfo> snapshots;
	idx_t current_row = 0;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> PaimonSnapshotsBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto bind_data = make_uniq<PaimonSnapshotsBindData>();

	bind_data->path = PaimonTablePath::Parse(input.inputs);
	for (auto &entry : input.named_parameters) {
		bind_data->input_options[entry.first.GetIdentifierName()] = entry.second;
	}

	names = {"snapshot_id", "schema_id",          "commit_user",        "commit_kind",
	         "commit_time", "total_record_count", "delta_record_count", "watermark"};

	return_types = {LogicalType::BIGINT,    LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::TIMESTAMP, LogicalType::BIGINT, LogicalType::BIGINT,  LogicalType::BIGINT};

	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> PaimonSnapshotsInitGlobal(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<PaimonSnapshotsBindData>();
	auto state = make_uniq<PaimonSnapshotsGlobalState>();

	auto paimon_catalog = PaimonCatalog::CreatePaimonCatalog(context, bind.path.warehouse, bind.input_options);

	paimon::Identifier identifier(bind.path.dbname, bind.path.tablename);
	auto result = paimon_catalog->ListSnapshots(identifier);
	if (!result.ok()) {
		throw IOException(result.status().ToString());
	}
	state->snapshots = std::move(result).value();

	return std::move(state);
}

static void PaimonSnapshotsExecute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<PaimonSnapshotsGlobalState>();

	idx_t count = 0;
	while (state.current_row < state.snapshots.size() && count < STANDARD_VECTOR_SIZE) {
		auto &snap = state.snapshots[state.current_row];

		output.data[0].Append(Value::BIGINT(snap.snapshot_id));
		output.data[1].Append(Value::BIGINT(snap.schema_id));
		output.data[2].Append(Value(snap.commit_user));
		output.data[3].Append(Value(paimon::SnapshotInfo::CommitKindToString(snap.commit_kind)));

		// timeMillis is epoch ms; DuckDB timestamp_t is epoch us
		timestamp_t ts;
		ts.value = snap.time_millis * 1000;
		output.data[4].Append(Value::TIMESTAMP(ts));

		output.data[5].Append(snap.total_record_count ? Value::BIGINT(snap.total_record_count.value()) : Value());
		output.data[6].Append(snap.delta_record_count ? Value::BIGINT(snap.delta_record_count.value()) : Value());
		output.data[7].Append(snap.watermark ? Value::BIGINT(snap.watermark.value()) : Value());

		state.current_row++;
		count++;
	}

	output.CheckCardinality(count);
}

static void AddPaimonSnapshotsThreePartFunction(CreateTableFunctionInfo &info) {
	auto fun = TableFunction("paimon_snapshots", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                         PaimonSnapshotsExecute, PaimonSnapshotsBind, PaimonSnapshotsInitGlobal);
	fun.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("manifest_format", LogicalType::VARCHAR); // deprecated: auto-detected from table schema
	});
	info.functions.AddFunction(fun);

	FunctionDescription desc;
	desc.parameter_names = {"warehouse", "database", "table"};
	desc.description = "List a Paimon table's snapshots, including snapshot IDs, commit times and record counts. "
	                   "manifest_format is deprecated; the format is detected from the table schema.";
	desc.examples = {"SELECT * FROM paimon_snapshots('./data', 'testdb', 'testtbl');"};
	desc.categories = {"paimon"};
	info.descriptions.push_back(std::move(desc));
}

static void AddPaimonSnapshotsFullPathFunction(CreateTableFunctionInfo &info) {
	auto fun_fullpath = TableFunction("paimon_snapshots", {LogicalType::VARCHAR}, PaimonSnapshotsExecute,
	                                  PaimonSnapshotsBind, PaimonSnapshotsInitGlobal);
	fun_fullpath.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("manifest_format", LogicalType::VARCHAR); // deprecated
	});
	info.functions.AddFunction(fun_fullpath);

	FunctionDescription desc_fullpath;
	desc_fullpath.parameter_names = {"table_path"};
	desc_fullpath.description =
	    "List a Paimon table's snapshots, including snapshot IDs, commit times and record counts. "
	    "manifest_format is deprecated; the format is detected from the table schema.";
	desc_fullpath.examples = {"SELECT * FROM paimon_snapshots('./data/testdb.db/testtbl');"};
	desc_fullpath.categories = {"paimon"};
	info.descriptions.push_back(std::move(desc_fullpath));
}

CreateTableFunctionInfo PaimonFunctions::GetPaimonSnapshotsFunction() {
	CreateTableFunctionInfo info(TableFunctionSet("paimon_snapshots"));
	info.on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
	AddPaimonSnapshotsThreePartFunction(info);
	AddPaimonSnapshotsFullPathFunction(info);
	return info;
}

} // namespace duckdb
