/*-------------------------------------------------------------------------
 *
 * paimon_functions.hpp
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
 *	  src/include/paimon_functions.hpp
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct PaimonTablePath {
	string warehouse;
	string dbname;
	string tablename;

	static PaimonTablePath Parse(const vector<Value> &inputs);
};

class PaimonFunctions {
public:
	static vector<TableFunctionSet> GetTableFunctions();

private:
	static TableFunctionSet GetPaimonScanFunction();
	static TableFunctionSet GetPaimonSnapshotsFunction();
};

} // namespace duckdb
