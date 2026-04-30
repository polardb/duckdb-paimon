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
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"

#include "paimon_catalog.hpp"
#include "paimon_schema_entry.hpp"
#include "paimon_transaction_manager.hpp"

#include <algorithm>
#include <optional>
#include <string>
#include <vector>

namespace duckdb {

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

map<string, string> PaimonCatalog::GetPaimonOptions(ClientContext &context, const string &path,
                                                    const unordered_map<string, Value> &input_options) {
	// Format options are only injected when the user explicitly provides them.
	// Otherwise paimon-cpp resolves the format from the table schema's own
	// options (with defaults: manifest.format=avro, file.format=parquet).
	static const vector<string> supported_manifest_formats = {"avro", "orc", "parquet"};
	static const vector<string> supported_file_formats = {"avro", "blob", "orc", "parquet"};

	map<string, string> paimon_options;

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

	// secret loading
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto secret_match = secret_manager.LookupSecret(transaction, path, "paimon");

	if (secret_match.HasMatch()) {
		const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_match.secret_entry->secret);

		auto ak = kv_secret.TryGetValue("key_id").ToString();
		auto sk = kv_secret.TryGetValue("secret").ToString();
		auto endpoint = kv_secret.TryGetValue("endpoint").ToString();

		// Extract bucket name from oss://bucketname/... path
		const string oss_prefix = "oss://";
		if (path.rfind(oss_prefix, 0) != 0) {
			throw IOException("Paimon secret found but path is not an OSS path: " + path);
		}
		auto bucket_start = oss_prefix.size();
		auto bucket_end = path.find('/', bucket_start);
		string bucket =
		    path.substr(bucket_start, bucket_end == string::npos ? string::npos : bucket_end - bucket_start);
		if (bucket.empty()) {
			throw IOException("Invalid OSS path, cannot extract bucket name: " + path);
		}

		paimon_options.insert({paimon::Options::FILE_SYSTEM, "jindo"});
		paimon_options.insert({"fs.oss.user", "paimon"});
		paimon_options.insert({"fs.oss.bucket." + bucket + ".accessKeyId", ak});
		paimon_options.insert({"fs.oss.bucket." + bucket + ".accessKeySecret", sk});
		paimon_options.insert({"fs.oss.bucket." + bucket + ".endpoint", endpoint});
	} else {
		paimon_options.insert({paimon::Options::FILE_SYSTEM, "local"});
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
	db.SetReadOnlyDatabase();
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

optional_ptr<CatalogEntry> PaimonCatalog::CreateSchema(CatalogTransaction, CreateSchemaInfo &) {
	throw NotImplementedException("CreateSchema not supported yet");
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

PhysicalOperator &PaimonCatalog::PlanCreateTableAs(ClientContext &, PhysicalPlanGenerator &, LogicalCreateTable &,
                                                   PhysicalOperator &) {
	throw NotImplementedException("PlanCreateTableAs not supported yet");
}

PhysicalOperator &PaimonCatalog::PlanInsert(ClientContext &, PhysicalPlanGenerator &, LogicalInsert &,
                                            optional_ptr<PhysicalOperator>) {
	throw NotImplementedException("PlanInsert not supported yet");
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

void PaimonCatalog::DropSchema(ClientContext &, DropInfo &) {
	throw NotImplementedException("DropSchema not supported yet");
}

} // namespace duckdb
