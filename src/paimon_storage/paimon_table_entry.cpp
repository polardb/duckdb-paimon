/*-------------------------------------------------------------------------
 *
 * paimon_table_entry.cpp
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
 *	  src/paimon_storage/paimon_table_entry.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "paimon_table_entry.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

#include "paimon_catalog.hpp"

namespace duckdb {

PaimonTableEntry::PaimonTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
}

unique_ptr<BaseStatistics> PaimonTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	throw NotImplementedException("GetStatistics not supported yet");
}

TableFunction PaimonTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, name);
	return GetScanFunction(context, bind_data, lookup);
}

TableFunction PaimonTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                                const EntryLookupInfo &lookup_info) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto transaction = CatalogTransaction::GetSystemTransaction(db);
	auto &catalog_schema = system_catalog.GetSchema(transaction, DEFAULT_SCHEMA);
	auto catalog_entry = catalog_schema.GetEntry(transaction, CatalogType::TABLE_FUNCTION_ENTRY, "paimon_scan");
	if (!catalog_entry) {
		throw InvalidInputException("Function \"paimon_scan\" not found");
	}
	auto &function_set = catalog_entry->Cast<TableFunctionCatalogEntry>();

	auto scan_function = function_set.functions.GetFunctionByArguments(
	    context, {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR});

	vector<Value> inputs = {Value(catalog.GetDBPath()), Value(schema.name), Value(name)};

	named_parameter_map_t param_map;
	auto &paimon_catalog = catalog.Cast<PaimonCatalog>();
	for (const auto &entry : paimon_catalog.GetAttachOptions()) {
		param_map[entry.first] = entry.second;
	}

	auto at_clause = lookup_info.GetAtClause();
	if (at_clause) {
		auto unit = StringUtil::Upper(at_clause->Unit());
		if (unit == "VERSION") {
			param_map["snapshot_from_id"] = at_clause->GetValue().CastAs(context, LogicalType::BIGINT);
		} else if (unit == "TIMESTAMP") {
			param_map["snapshot_from_timestamp"] = at_clause->GetValue().CastAs(context, LogicalType::TIMESTAMP);
		} else {
			throw InvalidInputException("Unsupported AT unit \"%s\" for Paimon. Use VERSION or TIMESTAMP.",
			                            at_clause->Unit());
		}
	}

	vector<LogicalType> return_types;
	vector<string> names;
	TableFunctionRef empty_ref;

	TableFunctionBindInput bind_input(inputs, param_map, return_types, names, nullptr, nullptr, scan_function,
	                                  empty_ref);
	bind_data = scan_function.bind(context, bind_input, return_types, names);

	return scan_function;
}

TableStorageInfo PaimonTableEntry::GetStorageInfo(ClientContext &context) {
	return TableStorageInfo();
}

virtual_column_map_t PaimonTableEntry::GetVirtualColumns() const {
	return {};
}

vector<column_t> PaimonTableEntry::GetRowIdColumns() const {
	return {};
}

} // namespace duckdb
