/*-------------------------------------------------------------------------
 *
 * paimon_schema_entry.cpp
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
 *	  src/paimon_storage/paimon_schema_entry.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include "paimon/catalog/identifier.h"

#include "paimon_catalog.hpp"
#include "paimon_schema_entry.hpp"
#include "paimon_table_set.hpp"

namespace duckdb {

PaimonSchemaEntry::PaimonSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info), tables(*this) {
}

static bool CatalogTypeIsSupported(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		return true;
	default:
		return false;
	}
}

void PaimonSchemaEntry::Scan(ClientContext &context, CatalogType type,
                             const std::function<void(CatalogEntry &)> &callback) {
	if (!CatalogTypeIsSupported(type)) {
		return;
	}

	auto &tables = GetTables();
	tables.Scan(context, callback);
}

void PaimonSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan without ClientContext not supported yet");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                          const EntryLookupInfo &lookup_info) {
	if (!CatalogTypeIsSupported(lookup_info.GetCatalogType())) {
		return nullptr;
	}

	auto &tables = GetTables();
	return tables.GetEntry(transaction.GetContext(), lookup_info);
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateIndex(CatalogTransaction, CreateIndexInfo &, TableCatalogEntry &) {
	throw NotImplementedException("CreateIndex not supported yet");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateFunction(CatalogTransaction, CreateFunctionInfo &) {
	throw NotImplementedException("CreateFunction not supported yet");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	auto &base = info.Base();
	if (base.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		throw NotImplementedException("CREATE OR REPLACE TABLE is not supported for Paimon tables");
	}

	auto &catalog = ParentCatalog().Cast<PaimonCatalog>();
	auto &paimon_catalog = catalog.GetPaimonCatalog();
	paimon::Identifier identifier(name, base.table);

	auto col_names = base.columns.GetColumnNames();
	auto col_types = base.columns.GetColumnTypes();

	ArrowSchema arrow_schema {};
	auto client_props = transaction.GetContext().GetClientProperties();
	ArrowConverter::ToArrowSchema(&arrow_schema, col_types, col_names, client_props);
	struct ArrowSchemaGuard {
		ArrowSchema &schema;
		~ArrowSchemaGuard() {
			if (schema.release) {
				schema.release(&schema);
			}
		}
	} arrow_guard {arrow_schema};

	vector<string> primary_keys;
	for (auto &constraint : base.constraints) {
		if (constraint->type != ConstraintType::UNIQUE) {
			continue;
		}
		auto &unique = constraint->Cast<UniqueConstraint>();
		if (!unique.IsPrimaryKey()) {
			continue;
		}
		if (unique.HasIndex()) {
			auto col_idx = unique.GetIndex();
			primary_keys.push_back(col_names[col_idx.index]);
		} else {
			for (auto &col_name : unique.GetColumnNames()) {
				primary_keys.push_back(col_name);
			}
		}
	}

	vector<string> partition_keys;
	for (auto &pk_expr : base.partition_keys) {
		if (pk_expr->GetExpressionType() == ExpressionType::COLUMN_REF) {
			partition_keys.push_back(pk_expr->Cast<ColumnRefExpression>().GetColumnName());
		} else {
			throw InvalidInputException("Paimon partition key must be a column reference");
		}
	}

	std::map<string, string> paimon_options;
	for (auto &opt : base.options) {
		auto &expr = *opt.second;
		if (expr.GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
			auto &val = expr.Cast<ConstantExpression>().value;
			if (!val.IsNull()) {
				paimon_options[opt.first] = val.ToString();
			}
		} else if (expr.GetExpressionType() == ExpressionType::COLUMN_REF) {
			paimon_options[opt.first] = expr.Cast<ColumnRefExpression>().GetColumnName();
		} else {
			throw InvalidInputException("Paimon table option '%s' must be a literal value", opt.first);
		}
	}

	bool ignore_if_exists = base.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT;
	auto status = paimon_catalog.CreateTable(identifier, &arrow_schema, partition_keys, primary_keys, paimon_options,
	                                         ignore_if_exists);

	if (!status.ok()) {
		if (status.IsExist() || status.IsNotExist()) {
			throw CatalogException(status.ToString());
		}
		throw IOException(status.ToString());
	}

	return tables.RefreshEntry(transaction.GetContext(), base.table);
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateView(CatalogTransaction, CreateViewInfo &) {
	throw NotImplementedException("CreateView not supported yet");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateSequence(CatalogTransaction, CreateSequenceInfo &) {
	throw NotImplementedException("CreateSequence not supported yet");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateTableFunction(CatalogTransaction, CreateTableFunctionInfo &) {
	throw NotImplementedException("CreateTableFunction not supported yet");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateCopyFunction(CatalogTransaction, CreateCopyFunctionInfo &) {
	throw NotImplementedException("CreateCopyFunction not supported yet");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreatePragmaFunction(CatalogTransaction, CreatePragmaFunctionInfo &) {
	throw NotImplementedException("CreatePragmaFunction not supported yet");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateCollation(CatalogTransaction, CreateCollationInfo &) {
	throw NotImplementedException("CreateCollation not supported yet");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateType(CatalogTransaction, CreateTypeInfo &) {
	throw NotImplementedException("CreateType not supported yet");
}

void PaimonSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	if (info.type != CatalogType::TABLE_ENTRY) {
		throw NotImplementedException("Only dropping tables is supported");
	}

	auto &catalog = ParentCatalog().Cast<PaimonCatalog>();
	auto &paimon_catalog = catalog.GetPaimonCatalog();
	paimon::Identifier identifier(name, info.name);

	bool ignore_if_not_exists = info.if_not_found == OnEntryNotFound::RETURN_NULL;
	auto status = paimon_catalog.DropTable(identifier, ignore_if_not_exists);
	if (!status.ok()) {
		if (status.IsExist() || status.IsNotExist()) {
			throw CatalogException(status.ToString());
		}
		throw IOException(status.ToString());
	}

	tables.DropEntry(info.name);
}

void PaimonSchemaEntry::Alter(CatalogTransaction, AlterInfo &) {
	throw NotImplementedException("Alter not supported yet");
}

} // namespace duckdb
