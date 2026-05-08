/*-------------------------------------------------------------------------
 *
 * paimon_schema_set.cpp
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
 *	  src/paimon_storage/paimon_schema_set.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

#include "paimon_catalog.hpp"
#include "paimon_schema_entry.hpp"
#include "paimon_schema_set.hpp"

namespace duckdb {

PaimonSchemaSet::PaimonSchemaSet(Catalog &catalog) : catalog(catalog) {
}

void PaimonSchemaSet::LoadEntries(ClientContext &context) {
	if (inited) {
		return;
	}

	auto &paimon_catalog = catalog.Cast<PaimonCatalog>().GetPaimonCatalog();

	auto databases_result = paimon_catalog.ListDatabases();
	if (!databases_result.ok()) {
		throw IOException(databases_result.status().ToString());
	}
	auto databases = std::move(databases_result).value();

	for (auto &database : databases) {
		CreateSchemaInfo info;
		info.schema = database;
		auto schema_entry = make_uniq<PaimonSchemaEntry>(catalog, info);
		entries.emplace(make_pair(database, std::move(schema_entry)));
	}

	inited = true;
}

void PaimonSchemaSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	lock_guard<mutex> l(entry_lock);
	LoadEntries(context);

	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> PaimonSchemaSet::GetEntry(ClientContext &context, const string &name) {
	lock_guard<mutex> l(entry_lock);
	LoadEntries(context);

	auto entry = entries.find(name);
	if (entry == entries.end()) {
		return nullptr;
	}

	return entry->second.get();
}

optional_ptr<CatalogEntry> PaimonSchemaSet::CreateEntry(const string &name) {
	lock_guard<mutex> l(entry_lock);
	CreateSchemaInfo info;
	info.schema = name;
	auto schema_entry = make_uniq<PaimonSchemaEntry>(catalog, info);
	auto result = entries.emplace(make_pair(name, std::move(schema_entry)));
	return result.first->second.get();
}

void PaimonSchemaSet::DropEntry(const string &name) {
	lock_guard<mutex> l(entry_lock);
	entries.erase(name);
}

} // namespace duckdb
