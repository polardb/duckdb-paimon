/*-------------------------------------------------------------------------
 *
 * paimon_catalog.hpp
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
 *	  src/include/paimon_catalog.hpp
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

#include "paimon/catalog/catalog.h"

#include "paimon_schema_set.hpp"

namespace duckdb {

class PaimonCatalog : public Catalog {
public:
	PaimonCatalog(ClientContext &context, AttachedDatabase &db, const string &path,
	              const unordered_map<string, Value> &, AccessMode access_mode);

	static unique_ptr<Catalog> Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
	                                  AttachedDatabase &db, const string &name, AttachInfo &info,
	                                  AttachOptions &options);
	static unique_ptr<TransactionManager> CreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
	                                                               AttachedDatabase &db, Catalog &catalog);

	static map<string, string> GetPaimonOptions(ClientContext &context, const string &path,
	                                            const unordered_map<string, Value> &input_options);
	static unique_ptr<paimon::Catalog> CreatePaimonCatalog(ClientContext &context, const string &path,
	                                                       const unordered_map<string, Value> &input_options);

	// catalog APIs
	void Initialize(bool load_builtin) override;
	string GetCatalogType() override;

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override;
	string GetDBPath() override;

	PaimonSchemaSet &GetSchemas() {
		return schemas;
	}

	paimon::Catalog &GetPaimonCatalog() {
		return *paimon_catalog;
	}

	const unordered_map<string, Value> &GetAttachOptions() const {
		return attached_options;
	}

private:
	string path;
	AccessMode access_mode;
	unordered_map<string, Value> attached_options;
	unique_ptr<paimon::Catalog> paimon_catalog;
	PaimonSchemaSet schemas;

	void DropSchema(ClientContext &context, DropInfo &info) override;
};

} // namespace duckdb
