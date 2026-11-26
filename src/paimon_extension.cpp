/*-------------------------------------------------------------------------
 *
 * paimon_extension.cpp
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
 *	  src/paimon_extension.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "paimon_catalog.hpp"
#include "paimon_extension.hpp"
#include "paimon_functions.hpp"
#include "paimon_scan_definitions.hpp"

#include <new>

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class PaimonStorageExtension : public StorageExtension {
public:
	PaimonStorageExtension() {
		attach = PaimonCatalog::Attach;
		create_transaction_manager = PaimonCatalog::CreateTransactionManager;
	}
};

static unique_ptr<BaseSecret> CreatePaimonSecretFromConfig(ClientContext &context, CreateSecretInput &input) {
	auto scope = input.scope;
	if (scope.empty()) {
		scope = {""};
	}
	auto result = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);

	result->TrySetValue("endpoint", input);
	result->TrySetValue("key_id", input);
	result->TrySetValue("secret", input);
	result->TrySetValue("session_token", input);
	result->TrySetValue("region", input);
	result->TrySetValue("profile", input);
	result->TrySetValue("path_style_access", input);
	result->TrySetValue("signature_version", input);

	result->redact_keys = {"key_id", "secret", "session_token"};

	return std::move(result);
}

static CreateTableFunctionInfo GetNormalizedPaimonScanFunction() {
	return PaimonFunctions::NormalizeTableFunction(PaimonFunctions::GetPaimonScanFunction());
}

static void LoadInternal(ExtensionLoader &loader) {
	SecretType secret_type;
	secret_type.name = "paimon";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";
	secret_type.extension = "paimon";
	loader.RegisterSecretType(secret_type);

	CreateSecretFunction secret_fun = {"paimon", "config", CreatePaimonSecretFromConfig};
	secret_fun.named_parameters["endpoint"] = LogicalType::VARCHAR;
	secret_fun.named_parameters["key_id"] = LogicalType::VARCHAR;
	secret_fun.named_parameters["secret"] = LogicalType::VARCHAR;
	secret_fun.named_parameters["session_token"] = LogicalType::VARCHAR;
	secret_fun.named_parameters["region"] = LogicalType::VARCHAR;
	secret_fun.named_parameters["path_style_access"] = LogicalType::BOOLEAN;
	secret_fun.named_parameters["signature_version"] = LogicalType::VARCHAR;
	loader.RegisterFunction(secret_fun);

	CreateSecretFunction credential_chain_fun = {"paimon", "credential_chain", CreatePaimonSecretFromConfig};
	credential_chain_fun.named_parameters["endpoint"] = LogicalType::VARCHAR;
	credential_chain_fun.named_parameters["region"] = LogicalType::VARCHAR;
	credential_chain_fun.named_parameters["profile"] = LogicalType::VARCHAR;
	credential_chain_fun.named_parameters["path_style_access"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(credential_chain_fun);

	loader.RegisterFunction(GetNormalizedPaimonScanFunction());
	loader.RegisterFunction(PaimonFunctions::NormalizeTableFunction(PaimonFunctions::GetPaimonSnapshotsFunction()));

	auto &instance = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(instance);
	StorageExtension::Register(config, "paimon", make_shared_ptr<PaimonStorageExtension>());
}

void PaimonExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

string PaimonExtension::Name() {
	return "paimon";
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(paimon, loader) {
	LoadInternal(loader);
}
}

#if defined(__linux__)
extern "C" DUCKDB_EXTENSION_API duckdb::PaimonScanDefinitionStatusV1
paimon_visit_scan_definitions_v1(void *context, duckdb::PaimonScanDefinitionVisitorV1 visitor) noexcept {
	using duckdb::PaimonScanDefinitionStatusV1;
	if (!visitor) {
		return PaimonScanDefinitionStatusV1::INVALID_ARGUMENT;
	}
	try {
		auto info = duckdb::GetNormalizedPaimonScanFunction();
		for (const auto &definition : info.functions.functions) {
			if (!visitor(context, definition.get())) {
				return PaimonScanDefinitionStatusV1::VISITOR_REJECTED;
			}
		}
		return PaimonScanDefinitionStatusV1::OK;
	} catch (const std::bad_alloc &) {
		return PaimonScanDefinitionStatusV1::OUT_OF_MEMORY;
	} catch (const duckdb::OutOfMemoryException &) {
		return PaimonScanDefinitionStatusV1::OUT_OF_MEMORY;
	} catch (...) {
		return PaimonScanDefinitionStatusV1::INTERNAL_ERROR;
	}
}
#endif
