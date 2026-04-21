/*-------------------------------------------------------------------------
 *
 * paimon_type_utils.hpp
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
 *	  src/include/paimon_type_utils.hpp
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "duckdb.hpp"

#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/predicate/literal.h"

#include <optional>

namespace duckdb {

/// Utility class for converting between DuckDB types and paimon types.
class PaimonTypeUtils {
public:
	/// Convert a DuckDB LogicalType to the corresponding paimon FieldType.
	/// Returns FieldType::UNKNOWN for unsupported types.
	static paimon::FieldType ConvertFieldType(const LogicalType &type);

	/// Convert a DuckDB Value to a paimon Literal for the given paimon field type.
	/// Returns std::nullopt if the type is not supported for literal conversion.
	/// Throws IOException if the value is NULL.
	static std::optional<paimon::Literal> ConvertLiteral(const Value &value, paimon::FieldType field_type);
};

} // namespace duckdb
