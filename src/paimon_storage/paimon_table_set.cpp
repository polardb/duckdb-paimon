/*-------------------------------------------------------------------------
 *
 * paimon_table_set.cpp
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
 *	  src/paimon_storage/paimon_table_set.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "duckdb.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include "paimon/schema/schema.h"

#include "paimon_catalog.hpp"
#include "paimon_schema_entry.hpp"
#include "paimon_table_entry.hpp"
#include "paimon_table_set.hpp"

namespace duckdb {

PaimonTableSet::PaimonTableSet(PaimonSchemaEntry &schema) : schema(schema) {
}

optional_ptr<CatalogEntry> PaimonTableSet::BuildEntry(ClientContext &context, const string &table_name) {
	auto entry_iter = entries.find(table_name);
	if (entry_iter != entries.end()) {
		return entry_iter->second.get();
	}

	auto &catalog = schema.ParentCatalog().Cast<PaimonCatalog>();
	auto &paimon_catalog = catalog.GetPaimonCatalog();

	auto table_schema_result = paimon_catalog.LoadTableSchema(paimon::Identifier(schema.name, table_name));
	if (!table_schema_result.ok()) {
		if (table_schema_result.status().IsNotExist()) {
			return nullptr;
		} else {
			throw IOException(table_schema_result.status().ToString());
		}
	}
	auto table_schema = std::move(table_schema_result).value();

	auto arrow_schema_result = table_schema->GetArrowSchema();
	if (!arrow_schema_result.ok()) {
		throw IOException(arrow_schema_result.status().ToString());
	}
	auto arrow_raw = std::move(arrow_schema_result).value();
	auto arrow_deleter = [](ArrowSchema *schema) {
		if (schema && schema->release) {
			schema->release(schema);
		}
		delete schema;
	};
	std::unique_ptr<ArrowSchema, decltype(arrow_deleter)> arrow_schema(arrow_raw.release(), arrow_deleter);

	ArrowTableSchema arrow_table;
	ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table, *arrow_schema);

	auto col_names = arrow_table.GetNames();
	auto col_types = arrow_table.GetTypes();
	if (col_names.size() != col_types.size()) {
		throw IOException("col_name: %d, col_types: %d", col_names.size(), col_types.size());
	}

	CreateTableInfo table_info;
	table_info.table = table_name;

	for (idx_t i = 0; i < col_names.size(); i++) {
		table_info.columns.AddColumn(ColumnDefinition(col_names[i], col_types[i]));
	}

	auto table_entry = make_uniq<PaimonTableEntry>(catalog, schema, table_info);
	auto [iter, inserted] = entries.emplace(make_pair(table_name, std::move(table_entry)));

	return iter->second.get();
}

void PaimonTableSet::LoadEntries(ClientContext &context) {
	if (inited) {
		return;
	}

	auto &catalog = schema.ParentCatalog().Cast<PaimonCatalog>();
	auto &paimon_catalog = catalog.GetPaimonCatalog();
	auto &schema_name = schema.name;

	auto tables_result = paimon_catalog.ListTables(schema_name);
	if (!tables_result.ok()) {
		throw IOException(tables_result.status().ToString());
	}
	auto tables = std::move(tables_result).value();

	for (auto &table_name : tables) {
		BuildEntry(context, table_name);
	}

	inited = true;
}

optional_ptr<CatalogEntry> PaimonTableSet::GetEntry(ClientContext &context, const EntryLookupInfo &lookup) {
	const auto &table_name = lookup.GetEntryName();
	lock_guard<mutex> l(entry_lock);
	return BuildEntry(context, table_name);
}

void PaimonTableSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	lock_guard<mutex> l(entry_lock);
	LoadEntries(context);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> PaimonTableSet::CreateEntry(CreateTableInfo &info) {
	lock_guard<mutex> l(entry_lock);
	auto &catalog = schema.ParentCatalog().Cast<PaimonCatalog>();
	auto table_entry = make_uniq<PaimonTableEntry>(catalog, schema, info);
	auto result = entries.emplace(make_pair(info.table, std::move(table_entry)));
	return result.first->second.get();
}

optional_ptr<CatalogEntry> PaimonTableSet::RefreshEntry(ClientContext &context, const string &table_name) {
	lock_guard<mutex> l(entry_lock);
	entries.erase(table_name);
	return BuildEntry(context, table_name);
}

void PaimonTableSet::DropEntry(const string &table_name) {
	lock_guard<mutex> l(entry_lock);
	entries.erase(table_name);
}

} // namespace duckdb
