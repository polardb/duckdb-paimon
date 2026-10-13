/*-------------------------------------------------------------------------
 *
 * paimon_functions.cpp
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
 *	  src/paimon_functions.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "paimon_functions.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "paimon/catalog/catalog.h"
#include "yyjson.hpp"

namespace duckdb {

// Field descriptions are annotations, including within nested ROW types.
static duckdb_yyjson::yyjson_mut_val *CopyColumnMetadata(duckdb_yyjson::yyjson_mut_doc *doc,
                                                         duckdb_yyjson::yyjson_val *value) {
	using namespace duckdb_yyjson;
	if (yyjson_is_obj(value)) {
		auto result = yyjson_mut_obj(doc);
		size_t idx, count;
		yyjson_val *key, *child;
		yyjson_obj_foreach(value, idx, count, key, child) {
			if (string(yyjson_get_str(key)) != "description") {
				yyjson_mut_obj_add(result, yyjson_val_mut_copy(doc, key), CopyColumnMetadata(doc, child));
			}
		}
		return result;
	}
	if (yyjson_is_arr(value)) {
		auto result = yyjson_mut_arr(doc);
		size_t idx, count;
		yyjson_val *child;
		yyjson_arr_foreach(value, idx, count, child) {
			yyjson_mut_arr_append(result, CopyColumnMetadata(doc, child));
		}
		return result;
	}
	return yyjson_val_mut_copy(doc, value);
}

string PaimonFunctions::GetColumnSignature(const string &schema_json) {
	using namespace duckdb_yyjson;
	std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> parsed(
	    yyjson_read(schema_json.c_str(), schema_json.size(), 0), yyjson_doc_free);
	if (!parsed) {
		throw IOException("Invalid Paimon schema JSON");
	}
	std::unique_ptr<yyjson_mut_doc, decltype(&yyjson_mut_doc_free)> signature(yyjson_mut_doc_new(nullptr),
	                                                                          yyjson_mut_doc_free);
	auto root = yyjson_mut_obj(signature.get());
	yyjson_mut_doc_set_root(signature.get(), root);
	for (auto key : {"fields", "partitionKeys", "primaryKeys"}) {
		auto value = yyjson_obj_get(yyjson_doc_get_root(parsed.get()), key);
		if (!yyjson_is_arr(value)) {
			throw IOException("Paimon schema is missing column metadata: %s", key);
		}
		yyjson_mut_obj_add_val(signature.get(), root, key, CopyColumnMetadata(signature.get(), value));
	}
	std::unique_ptr<char, decltype(&free)> json(yyjson_mut_write(signature.get(), 0, nullptr), free);
	if (!json) {
		throw IOException("Could not serialize Paimon column metadata");
	}
	return string(json.get());
}

PaimonTablePath PaimonTablePath::Parse(const vector<Value> &inputs) {
	PaimonTablePath result;

	if (inputs.empty()) {
		throw InvalidInputException("warehouse path is necessary");
	}

	if (inputs.size() > 1) {
		result.warehouse = inputs[0].ToString();
		result.dbname = inputs[1].ToString();
		result.tablename = inputs[2].ToString();
	} else {
		auto whole_path = inputs[0].ToString();
		auto last_slash = whole_path.find_last_of('/');

		if (last_slash == string::npos) {
			throw InvalidInputException("Invalid database path format: missing '/'");
		}

		result.tablename = whole_path.substr(last_slash + 1);

		whole_path = whole_path.substr(0, last_slash);
		last_slash = whole_path.find_last_of('/');

		string dbname_raw = whole_path.substr(last_slash + 1);
		string dbsuffix(paimon::Catalog::DB_SUFFIX);

		if (!StringUtil::EndsWith(dbname_raw, dbsuffix)) {
			throw InvalidInputException("Invalid database path format");
		}

		result.dbname = dbname_raw.substr(0, dbname_raw.size() - dbsuffix.size());
		result.warehouse = whole_path.substr(0, last_slash);
	}

	return result;
}

void PaimonFunctions::RegisterTableFunction(ExtensionLoader &loader, CreateTableFunctionInfo info) {
	auto name = info.name;
	loader.RegisterFunction(std::move(info));
	auto &entry = loader.GetTableFunction(name);

	for (idx_t i = 0; i < entry.functions.Size(); i++) {
		// Match the copy used by duckdb_functions() to generate parameter types.
		auto fun = entry.functions.GetFunctionByOffset(i);
		auto &desc = entry.descriptions[i];
		for (const auto &param : fun.named_parameters) {
			desc.parameter_names.push_back(param.first);
		}
	}
}

} // namespace duckdb
