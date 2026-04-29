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

#include "paimon/catalog/catalog.h"

namespace duckdb {

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

vector<TableFunctionSet> PaimonFunctions::GetTableFunctions() {
	vector<TableFunctionSet> functions;

	functions.push_back(GetPaimonScanFunction());
	functions.push_back(GetPaimonSnapshotsFunction());

	return functions;
}

} // namespace duckdb
