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

#include "duckdb.hpp"
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

	result->redact_keys = {"key_id", "secret"};

	return std::move(result);
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
	loader.RegisterFunction(secret_fun);

	for (auto &fun : PaimonFunctions::GetTableFunctions()) {
		loader.RegisterFunction(fun);
	}

	auto &instance = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(instance);
	config.storage_extensions["paimon"] = make_uniq<PaimonStorageExtension>();
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
