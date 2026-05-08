/*-------------------------------------------------------------------------
 *
 * paimon_schema_set.hpp
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
 *	  src/include/paimon_schema_set.hpp
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

class PaimonSchemaSet {
public:
	explicit PaimonSchemaSet(Catalog &catalog);

public:
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const string &name);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	optional_ptr<CatalogEntry> CreateEntry(const string &name);
	void DropEntry(const string &name);

private:
	Catalog &catalog;
	bool inited = false;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> entries;
	mutex entry_lock;

private:
	void LoadEntries(ClientContext &context);
};

} // namespace duckdb
