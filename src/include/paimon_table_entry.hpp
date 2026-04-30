/*-------------------------------------------------------------------------
 *
 * paimon_table_entry.hpp
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
 *	  src/include/paimon_table_entry.hpp
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

class PaimonTableEntry : public TableCatalogEntry {
public:
	PaimonTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info);

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                              const EntryLookupInfo &lookup_info) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
	virtual_column_map_t GetVirtualColumns() const override;
	vector<column_t> GetRowIdColumns() const override;
};

} // namespace duckdb
