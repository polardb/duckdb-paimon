/*-------------------------------------------------------------------------
 *
 * paimon_type_utils.cpp
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
 *	  src/paimon_storage/paimon_type_utils.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "paimon_type_utils.hpp"

namespace duckdb {

paimon::FieldType PaimonTypeUtils::ConvertFieldType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return paimon::FieldType::BOOLEAN;
	case LogicalTypeId::TINYINT:
		return paimon::FieldType::TINYINT;
	case LogicalTypeId::SMALLINT:
		return paimon::FieldType::SMALLINT;
	case LogicalTypeId::INTEGER:
		return paimon::FieldType::INT;
	case LogicalTypeId::BIGINT:
		return paimon::FieldType::BIGINT;
	case LogicalTypeId::FLOAT:
		return paimon::FieldType::FLOAT;
	case LogicalTypeId::DOUBLE:
		return paimon::FieldType::DOUBLE;
	case LogicalTypeId::VARCHAR:
		return paimon::FieldType::STRING;
	case LogicalTypeId::BLOB:
		return paimon::FieldType::BINARY;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_NS:
		return paimon::FieldType::TIMESTAMP;
	case LogicalTypeId::DECIMAL:
		return paimon::FieldType::DECIMAL;
	case LogicalTypeId::DATE:
		return paimon::FieldType::DATE;
	default:
		return paimon::FieldType::UNKNOWN;
	}
}

std::optional<paimon::Literal> PaimonTypeUtils::ConvertLiteral(const Value &value, paimon::FieldType field_type) {
	if (value.IsNull()) {
		throw IOException("literal cannot be NULL");
	}

	switch (field_type) {
	case paimon::FieldType::BOOLEAN:
		return paimon::Literal(value.GetValue<bool>());
	case paimon::FieldType::TINYINT:
		return paimon::Literal(value.GetValue<int8_t>());
	case paimon::FieldType::SMALLINT:
		return paimon::Literal(value.GetValue<int16_t>());
	case paimon::FieldType::INT:
		return paimon::Literal(value.GetValue<int32_t>());
	case paimon::FieldType::BIGINT:
		return paimon::Literal(value.GetValue<int64_t>());
	case paimon::FieldType::FLOAT:
		return paimon::Literal(value.GetValue<float>());
	case paimon::FieldType::DOUBLE:
		return paimon::Literal(value.GetValue<double>());
	case paimon::FieldType::STRING: {
		auto str = value.GetValue<string>();
		return paimon::Literal(paimon::FieldType::STRING, str.c_str(), str.size());
	}
	case paimon::FieldType::DATE:
		return paimon::Literal(paimon::FieldType::DATE, value.GetValue<int32_t>());
	case paimon::FieldType::TIMESTAMP: {
		int64_t millis;
		int32_t nano_of_millis;
		switch (value.type().id()) {
		case LogicalTypeId::TIMESTAMP_MS:
			millis = value.GetValue<timestamp_t>().value;
			nano_of_millis = 0;
			break;
		case LogicalTypeId::TIMESTAMP_SEC:
			millis = value.GetValue<timestamp_t>().value * 1000;
			nano_of_millis = 0;
			break;
		case LogicalTypeId::TIMESTAMP_NS: {
			auto nanos = value.GetValue<timestamp_t>().value;
			millis = nanos / 1000000 - (nanos % 1000000 < 0 ? 1 : 0);
			nano_of_millis = static_cast<int32_t>(nanos - millis * 1000000);
			break;
		}
		default: {
			auto micros = value.GetValue<timestamp_t>().value;
			millis = micros / 1000 - (micros % 1000 < 0 ? 1 : 0);
			nano_of_millis = static_cast<int32_t>((micros - millis * 1000) * 1000);
			break;
		}
		}
		return paimon::Literal(paimon::Timestamp::FromEpochMillis(millis, nano_of_millis));
	}
	case paimon::FieldType::DECIMAL: {
		auto width = DecimalType::GetWidth(value.type());
		auto scale = DecimalType::GetScale(value.type());
		hugeint_t hugeint_val;
		switch (value.type().InternalType()) {
		case PhysicalType::INT16:
			hugeint_val = Hugeint::Convert(value.GetValueUnsafe<int16_t>());
			break;
		case PhysicalType::INT32:
			hugeint_val = Hugeint::Convert(value.GetValueUnsafe<int32_t>());
			break;
		case PhysicalType::INT64:
			hugeint_val = Hugeint::Convert(value.GetValueUnsafe<int64_t>());
			break;
		case PhysicalType::INT128:
			hugeint_val = value.GetValueUnsafe<hugeint_t>();
			break;
		default:
			return std::nullopt;
		}
		paimon::Decimal::int128_t raw = (static_cast<paimon::Decimal::int128_t>(hugeint_val.upper) << 64) |
		                                static_cast<paimon::Decimal::uint128_t>(hugeint_val.lower);
		return paimon::Literal(paimon::Decimal(width, scale, raw));
	}
	default:
		return std::nullopt;
	}
}

} // namespace duckdb
