/*-------------------------------------------------------------------------
 *
 * paimon_scan_definitions.hpp
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
 *-------------------------------------------------------------------------
 */

#pragma once

#include <cstdint>

namespace duckdb {

class TableFunction;

enum class PaimonScanDefinitionStatusV1 : int32_t {
	OK = 0,
	INVALID_ARGUMENT = 1,
	VISITOR_REJECTED = 2,
	OUT_OF_MEMORY = 3,
	INTERNAL_ERROR = 4,
};

using PaimonScanDefinitionVisitorV1 = bool (*)(void *context, const TableFunction *definition) noexcept;

} // namespace duckdb

// Optional Linux loadable extension symbol, resolved on the verified, already loaded Paimon module.
// C linkage names the symbol; TableFunction requires the exact matching DuckDB C++ ABI.
// Visits the two normalized paimon_scan overloads, without catalog qualification. Identify each
// by signature. The borrowed definition is valid only during the callback. Keep the module loaded
// while using or destroying copies. A false visitor result stops iteration immediately; discard
// any staged copies on failure. The producer never invokes bind or scan callbacks here.
extern "C" duckdb::PaimonScanDefinitionStatusV1
paimon_visit_scan_definitions_v1(void *context, duckdb::PaimonScanDefinitionVisitorV1 visitor) noexcept;
