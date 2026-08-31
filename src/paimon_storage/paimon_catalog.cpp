/*-------------------------------------------------------------------------
 *
 * paimon_catalog.cpp
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
 *	  src/paimon_storage/paimon_catalog.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include "paimon/catalog/catalog.h"
#include "paimon/catalog_options.h"
#include "paimon/schema/schema.h"

#include "paimon_catalog.hpp"
#include "paimon_insert.hpp"
#include "paimon_schema_entry.hpp"
#include "paimon_transaction_manager.hpp"

#include <algorithm>
#include <optional>
#include <string>
#include <vector>

namespace duckdb {

static constexpr const char *S3_PATH_PREFIX = "s3://";
static constexpr const char *OSS_PATH_PREFIX = "oss://";

static std::optional<string> TryGetPaimonOptionValue(const unordered_map<string, Value> &input_options,
                                                     const string &key) {
	for (const auto &entry : input_options) {
		if (StringUtil::CIEquals(entry.first, key)) {
			return entry.second.ToString();
		}
	}
	return {};
}

static string GetValidatedFormatOption(const unordered_map<string, Value> &input_options, const string &key,
                                       const string &default_value, const vector<string> &supported_values) {
	auto raw_value = TryGetPaimonOptionValue(input_options, key);
	if (!raw_value.has_value()) {
		return default_value;
	}

	auto normalized_value = StringUtil::Lower(raw_value.value());
	StringUtil::Trim(normalized_value);
	if (normalized_value.empty()) {
		throw InvalidInputException("Option \"%s\" cannot be empty", key);
	}

	if (std::find(supported_values.begin(), supported_values.end(), normalized_value) == supported_values.end()) {
		throw InvalidInputException("Invalid value \"%s\" for option \"%s\". Supported values are: %s",
		                            raw_value.value(), key, StringUtil::Join(supported_values, ", "));
	}
	return normalized_value;
}

static void AddOptionalSecretOption(const KeyValueSecret &secret, const string &secret_key, const string &option_key,
                                    map<string, string> &paimon_options) {
	Value value;
	if (secret.TryGetValue(secret_key, value)) {
		paimon_options[option_key] = value.ToString();
	}
}

static void AddRequiredSecretOption(const KeyValueSecret &secret, const string &secret_key, const string &option_key,
                                    map<string, string> &paimon_options) {
	Value value;
	if (!secret.TryGetValue(secret_key, value) || value.ToString().empty()) {
		throw InvalidInputException("Missing required Paimon secret option \"%s\"", secret_key);
	}
	paimon_options[option_key] = value.ToString();
}

map<string, string> PaimonCatalog::GetPaimonOptions(ClientContext &context, const string &path,
                                                    const unordered_map<string, Value> &input_options) {
	// Format options are only injected when the user explicitly provides them.
	// Otherwise paimon-cpp resolves the format from the table schema's own
	// options (with defaults: manifest.format=avro, file.format=parquet).
	static const vector<string> supported_manifest_formats = {"avro", "orc", "parquet"};
	static const vector<string> supported_file_formats = {"avro", "blob", "orc", "parquet"};

	map<string, string> paimon_options;

	auto metastore = TryGetPaimonOptionValue(input_options, "metastore");
	if (metastore.has_value()) {
		paimon_options[paimon::CatalogOptions::METASTORE] = metastore.value();

		if (StringUtil::CIEquals(metastore.value(), "rest")) {
			auto uri = TryGetPaimonOptionValue(input_options, "uri");
			if (!uri.has_value() || uri->empty()) {
				throw InvalidInputException("URI is required when METASTORE is 'rest'");
			}
			paimon_options[paimon::CatalogOptions::URI] = uri.value();

			auto token_provider = TryGetPaimonOptionValue(input_options, "token_provider");
			if (!token_provider.has_value() || token_provider->empty()) {
				throw InvalidInputException("TOKEN_PROVIDER is required when METASTORE is 'rest'");
			}
			paimon_options[paimon::CatalogOptions::TOKEN_PROVIDER] = token_provider.value();

			auto token = TryGetPaimonOptionValue(input_options, "token");
			if (StringUtil::CIEquals(token_provider.value(), "bear") && (!token.has_value() || token->empty())) {
				throw InvalidInputException("TOKEN is required when TOKEN_PROVIDER is 'bear'");
			}
			if (token.has_value()) {
				paimon_options[paimon::CatalogOptions::TOKEN] = token.value();
			}
		}
	}

	auto manifest_fmt = TryGetPaimonOptionValue(input_options, "manifest_format");
	if (manifest_fmt.has_value()) {
		paimon_options[paimon::Options::MANIFEST_FORMAT] =
		    GetValidatedFormatOption(input_options, "manifest_format", "", supported_manifest_formats);
	}

	auto file_fmt = TryGetPaimonOptionValue(input_options, "file_format");
	if (file_fmt.has_value()) {
		paimon_options[paimon::Options::FILE_FORMAT] =
		    GetValidatedFormatOption(input_options, "file_format", "", supported_file_formats);
	}

	auto normalized_path = StringUtil::Lower(path);
	const bool is_oss = StringUtil::StartsWith(normalized_path, OSS_PATH_PREFIX);
	const bool is_s3 = StringUtil::StartsWith(normalized_path, S3_PATH_PREFIX);

	if (is_oss) {
		paimon_options[paimon::Options::FILE_SYSTEM] = "oss";
	} else if (is_s3) {
		paimon_options[paimon::Options::FILE_SYSTEM] = "s3";
	} else {
		paimon_options[paimon::Options::FILE_SYSTEM] = "local";
	}

	// Secret loading
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto secret_match = secret_manager.LookupSecret(transaction, path, "paimon");

	if (secret_match.HasMatch()) {
		const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_match.secret_entry->secret);
		const auto &provider = kv_secret.GetProvider();

		if (provider == "credential_chain" && !is_s3) {
			throw InvalidInputException("Paimon provider \"%s\" is only supported for S3 paths", provider);
		}

		if (is_oss) {
			AddRequiredSecretOption(kv_secret, "key_id", "fs.oss.accessKeyId", paimon_options);
			AddRequiredSecretOption(kv_secret, "secret", "fs.oss.accessKeySecret", paimon_options);
			AddOptionalSecretOption(kv_secret, "session_token", "fs.oss.sessionToken", paimon_options);
			AddOptionalSecretOption(kv_secret, "region", "fs.oss.region", paimon_options);
			AddOptionalSecretOption(kv_secret, "endpoint", "fs.oss.endpoint", paimon_options);
			AddOptionalSecretOption(kv_secret, "path_style_access", "fs.oss.usePathStyle", paimon_options);
			AddOptionalSecretOption(kv_secret, "signature_version", "fs.oss.signatureVersion", paimon_options);
		} else if (is_s3) {
			AddOptionalSecretOption(kv_secret, "key_id", "s3.access-key", paimon_options);
			AddOptionalSecretOption(kv_secret, "secret", "s3.secret-key", paimon_options);
			AddOptionalSecretOption(kv_secret, "endpoint", "s3.endpoint", paimon_options);
			AddOptionalSecretOption(kv_secret, "session_token", "s3.session.token", paimon_options);
			AddOptionalSecretOption(kv_secret, "profile", "s3.profile", paimon_options);
			AddOptionalSecretOption(kv_secret, "region", "s3.region", paimon_options);
			AddOptionalSecretOption(kv_secret, "path_style_access", "s3.path-style-access", paimon_options);
		}
	}

	paimon_options.insert({paimon::Options::READ_BATCH_SIZE, std::to_string(STANDARD_VECTOR_SIZE)});

	auto snap_id = TryGetPaimonOptionValue(input_options, "snapshot_from_id");
	auto snap_ts = TryGetPaimonOptionValue(input_options, "snapshot_from_timestamp");
	if (snap_id.has_value() && snap_ts.has_value()) {
		throw InvalidInputException("Cannot specify both 'snapshot_from_id' and 'snapshot_from_timestamp'");
	}
	if (snap_id.has_value()) {
		paimon_options[paimon::Options::SCAN_SNAPSHOT_ID] = snap_id.value();
	}
	if (snap_ts.has_value()) {
		auto ts = Timestamp::FromString(snap_ts.value(), false);
		auto epoch_ms = Timestamp::GetEpochMs(ts);
		paimon_options[paimon::Options::SCAN_TIMESTAMP_MILLIS] = std::to_string(epoch_ms);
	}

	return paimon_options;
}

unique_ptr<paimon::Catalog> PaimonCatalog::CreatePaimonCatalog(ClientContext &context, const string &path,
                                                               const unordered_map<string, Value> &input_options) {
	auto paimon_options = PaimonCatalog::GetPaimonOptions(context, path, input_options);

	auto result = paimon::Catalog::Create(path, paimon_options);
	if (!result.ok()) {
		throw IOException(result.status().ToString());
	}

	return unique_ptr<paimon::Catalog>(std::move(result).value().release());
}

PaimonCatalog::PaimonCatalog(ClientContext &context, AttachedDatabase &db, const string &path,
                             const unordered_map<string, Value> &attach_options, AccessMode access_mode)
    : Catalog(db), path(path), access_mode(access_mode), attached_options(attach_options),
      paimon_catalog(CreatePaimonCatalog(context, path, attach_options)), schemas(*this) {
}

unique_ptr<Catalog> PaimonCatalog::Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                          AttachedDatabase &db, const string &name, AttachInfo &info,
                                          AttachOptions &options) {
	auto normalized_path = StringUtil::Lower(info.path);
	if (StringUtil::StartsWith(normalized_path, S3_PATH_PREFIX) ||
	    StringUtil::StartsWith(normalized_path, OSS_PATH_PREFIX)) {
		options.access_mode = AccessMode::READ_ONLY;
	}
	if (options.access_mode == AccessMode::READ_ONLY) {
		db.SetReadOnlyDatabase();
	}
	return make_uniq<PaimonCatalog>(context, db, info.path, info.options, options.access_mode);
}

unique_ptr<TransactionManager> PaimonCatalog::CreateTransactionManager(optional_ptr<StorageExtensionInfo>,
                                                                       AttachedDatabase &db, Catalog &) {
	return make_uniq<PaimonTransactionManager>(db);
}

void PaimonCatalog::Initialize(bool load_builtin) {
}

string PaimonCatalog::GetCatalogType() {
	return "paimon";
}

optional_ptr<CatalogEntry> PaimonCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	bool ignore_if_exists = info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT;
	auto status = paimon_catalog->CreateDatabase(info.schema, {}, ignore_if_exists);
	if (!status.ok()) {
		if (status.IsExist() || status.IsNotExist()) {
			throw CatalogException(status.ToString());
		}
		throw IOException(status.ToString());
	}
	return schemas.CreateEntry(info.schema);
}

optional_ptr<SchemaCatalogEntry> PaimonCatalog::LookupSchema(CatalogTransaction transaction,
                                                             const EntryLookupInfo &schema_lookup,
                                                             OnEntryNotFound if_not_found) {
	auto &schema_name = schema_lookup.GetEntryName();
	auto entry = schemas.GetEntry(transaction.GetContext(), schema_name);

	if (!entry) {
		if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
			throw CatalogException(schema_lookup.GetErrorContext(), "Schema with name \"%s\" not found", schema_name);
		}
		return nullptr;
	}

	return &entry->Cast<SchemaCatalogEntry>();
}

void PaimonCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	auto &_schemas = GetSchemas();
	_schemas.Scan(context, [&](CatalogEntry &schema) { callback(schema.Cast<PaimonSchemaEntry>()); });
}

PhysicalOperator &PaimonCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   LogicalCreateTable &op, PhysicalOperator &plan) {
	if (access_mode == AccessMode::READ_ONLY) {
		throw PermissionException("Cannot write to a read-only Paimon catalog");
	}

	auto &base = op.info->Base();
	paimon::Identifier table_identifier(base.schema, base.table);
	auto paimon_options = GetPaimonOptions(context, path, attached_options);

	vector<string> part_keys;
	for (auto &part_expr : base.partition_keys) {
		if (part_expr->GetExpressionType() != ExpressionType::COLUMN_REF) {
			throw InvalidInputException("Paimon partition key must be a column reference");
		}
		part_keys.push_back(part_expr->Cast<ColumnRefExpression>().GetColumnName());
	}

	auto &insert = planner.Make<PhysicalPaimonInsert>(op, op.schema, std::move(op.info), std::move(table_identifier),
	                                                  std::move(paimon_options), std::move(part_keys), 0U);
	insert.children.push_back(plan);
	return insert;
}

PhysicalOperator &PaimonCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                            optional_ptr<PhysicalOperator> plan) {
	if (access_mode == AccessMode::READ_ONLY) {
		throw PermissionException("Cannot write to a read-only Paimon catalog");
	}

	auto &table = op.table;
	paimon::Identifier table_identifier(table.schema.name, table.name);
	auto paimon_options = GetPaimonOptions(context, path, attached_options);

	vector<string> part_keys;
	auto schema_result = paimon_catalog->LoadTableSchema(table_identifier);
	if (!schema_result.ok()) {
		throw IOException(schema_result.status().ToString());
	}
	auto data_schema = std::dynamic_pointer_cast<paimon::DataSchema>(schema_result.value());
	if (data_schema) {
		auto &schema_part_keys = data_schema->PartitionKeys();
		part_keys.assign(schema_part_keys.begin(), schema_part_keys.end());
	}

	if (plan && !op.column_index_map.empty()) {
		plan = planner.ResolveDefaultsProjection(op, *plan);
	}

	auto &insert = planner.Make<PhysicalPaimonInsert>(op, table.schema, nullptr, std::move(table_identifier),
	                                                  std::move(paimon_options), std::move(part_keys), 0U);
	if (plan) {
		insert.children.push_back(*plan);
	}
	return insert;
}

PhysicalOperator &PaimonCatalog::PlanDelete(ClientContext &, PhysicalPlanGenerator &, LogicalDelete &,
                                            PhysicalOperator &) {
	throw NotImplementedException("PlanDelete not supported yet");
}

PhysicalOperator &PaimonCatalog::PlanUpdate(ClientContext &, PhysicalPlanGenerator &, LogicalUpdate &,
                                            PhysicalOperator &) {
	throw NotImplementedException("PlanUpdate not supported yet");
}

DatabaseSize PaimonCatalog::GetDatabaseSize(ClientContext &) {
	throw NotImplementedException("GetDatabaseSize not supported yet");
}

ErrorData PaimonCatalog::SupportsCreateTable(BoundCreateTableInfo &info) {
	auto &base = info.Base();
	if (!base.sort_keys.empty()) {
		return ErrorData(ExceptionType::CATALOG,
		                 StringUtil::Format("SORTED BY is not supported for tables in a %s catalog", GetCatalogType()));
	}
	return ErrorData();
}

void PaimonCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	bool ignore_if_not_exists = info.if_not_found == OnEntryNotFound::RETURN_NULL;
	auto status = paimon_catalog->DropDatabase(info.name, ignore_if_not_exists, info.cascade);
	if (!status.ok()) {
		if (status.IsExist() || status.IsNotExist()) {
			throw CatalogException(status.ToString());
		}
		throw IOException(status.ToString());
	}
	schemas.DropEntry(info.name);
}

} // namespace duckdb
