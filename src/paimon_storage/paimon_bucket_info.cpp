/*
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
 */

#include "paimon_bucket_info.hpp"
#include "paimon_type_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "paimon/defs.h"

#include <limits>

namespace duckdb {

PaimonBucketInfo PaimonBucketInfo::Bind(const vector<string> &names, const vector<LogicalType> &types,
                                        const vector<string> &partition_keys, const std::vector<string> &primary_keys,
                                        const std::map<string, string> &options) {
	PaimonBucketInfo result;
	result.is_pk_table = !primary_keys.empty();
	for (auto &key : primary_keys) {
		auto field = std::find(names.begin(), names.end(), key);
		if (field == names.end()) {
			throw InvalidInputException("Paimon primary key '%s' not found in table schema", key);
		}
		result.primary_key_ids.push_back(std::distance(names.begin(), field));
	}
	auto bucket = options.find(paimon::Options::BUCKET);
	if (bucket != options.end()) {
		// Parse the option separately from validating Paimon's bucket modes.
		size_t end;
		int64_t count;
		try {
			count = std::stoll(bucket->second, &end);
		} catch (const std::invalid_argument &) {
			throw InvalidInputException("Paimon 'bucket' must be an integer");
		} catch (const std::out_of_range &) {
			throw InvalidInputException("Paimon 'bucket' must fit in a signed 32-bit integer");
		}
		// Reject trailing characters instead of accepting a numeric prefix.
		if (end != bucket->second.size()) {
			throw InvalidInputException("Paimon 'bucket' must be an integer");
		}
		// paimon-cpp represents the bucket count as int32_t.
		if (count < std::numeric_limits<int32_t>::min() || count > std::numeric_limits<int32_t>::max()) {
			throw InvalidInputException("Paimon 'bucket' must fit in a signed 32-bit integer");
		}
		// Fixed bucketing requires at least one bucket.
		if (count == 0) {
			throw InvalidInputException("Paimon 'bucket' cannot be 0; fixed buckets require a positive count");
		}
		// The only defined negative modes are unaware/dynamic (-1) and postponed (-2).
		if (count < -2) {
			throw InvalidInputException("Paimon 'bucket' values below -2 are not supported");
		}
		// Postponed bucketing is only a valid table definition for primary-key tables.
		if (count == -2 && !result.is_pk_table) {
			throw InvalidInputException("Paimon append tables do not support postponed buckets (bucket = -2)");
		}
		result.num_buckets = static_cast<int32_t>(count);
	}

	auto function = options.find(paimon::Options::BUCKET_FUNCTION_TYPE);
	if (function != options.end()) {
		result.function = StringUtil::Lower(function->second);
	}
	if (result.function != "default" && result.function != "mod" && result.function != "hive") {
		throw InvalidInputException("Unsupported Paimon bucket-function.type '%s'; expected default, mod or hive",
		                            result.function);
	}

	auto keys = options.find(paimon::Options::BUCKET_KEY);
	if (result.num_buckets == -1) {
		if (keys != options.end()) {
			throw InvalidInputException("Paimon tables with bucket = -1 cannot specify 'bucket-key'");
		}
		return result;
	}
	if (result.is_pk_table) {
		for (auto &key : partition_keys) {
			if (std::find(primary_keys.begin(), primary_keys.end(), key) == primary_keys.end()) {
				throw InvalidInputException(
				    "Paimon fixed/postpone-bucket primary-key tables require all partition keys in the primary key");
			}
		}
	}
	vector<string> bucket_keys;
	string key_option = keys == options.end() ? "" : keys->second;
	StringUtil::Trim(key_option);
	if (!key_option.empty()) {
		bucket_keys = StringUtil::Split(keys->second, ',');
	} else if (result.is_pk_table) {
		// Paimon defaults to primary keys in declaration order, excluding partition keys.
		for (auto &key : primary_keys) {
			if (std::find(partition_keys.begin(), partition_keys.end(), key) == partition_keys.end()) {
				bucket_keys.push_back(key);
			}
		}
	} else {
		throw InvalidInputException("Paimon fixed-bucket append tables require 'bucket-key'");
	}
	if (bucket_keys.empty()) {
		throw InvalidInputException("Paimon primary key must contain a non-partition column");
	}
	for (auto &key : bucket_keys) {
		if (result.is_pk_table && std::find(primary_keys.begin(), primary_keys.end(), key) == primary_keys.end()) {
			throw InvalidInputException("Paimon bucket key '%s' must be a primary-key column", key);
		}
		auto field = std::find(names.begin(), names.end(), key);
		if (field == names.end()) {
			throw InvalidInputException("Paimon bucket key '%s' not found in table schema", key);
		}
		if (std::find(partition_keys.begin(), partition_keys.end(), key) != partition_keys.end()) {
			throw InvalidInputException("Paimon bucket key '%s' cannot be a partition key", key);
		}
		auto index = std::distance(names.begin(), field);
		result.column_ids.push_back(index);
		result.column_names.push_back(key);
		result.column_types.push_back(types[index]);
	}
	// Validate the calculator before CREATE TABLE persists metadata or a writer creates files.
	if (result.num_buckets > 0) {
		result.CreateCalculator();
	}
	return result;
}

void PaimonBucketInfo::CheckWriteSupported() const {
	if (is_pk_table && num_buckets == -1) {
		throw NotImplementedException("Writing to Paimon primary-key tables with dynamic buckets (bucket = -1) is not "
		                              "supported by paimon-cpp; use a positive bucket count");
	}
	if (is_pk_table && num_buckets == -2) {
		throw NotImplementedException("Synchronous SQL writes to Paimon primary-key tables with postponed buckets "
		                              "(bucket = -2) are not supported; use a positive bucket count");
	}
}

std::unique_ptr<paimon::BucketIdCalculator> PaimonBucketInfo::CreateCalculator() const {
	auto pool = paimon::GetDefaultPool();
	auto create = [&]() -> paimon::Result<std::unique_ptr<paimon::BucketIdCalculator>> {
		if (function == "mod") {
			if (column_types.size() != 1) {
				throw InvalidInputException("Paimon mod bucketing requires exactly one INTEGER or BIGINT bucket key");
			}
			return paimon::BucketIdCalculator::CreateMod(is_pk_table, num_buckets,
			                                             PaimonTypeUtils::ConvertFieldType(column_types[0]), pool);
		}
		if (function == "hive") {
			std::vector<paimon::HiveFieldInfo> fields;
			for (auto &type : column_types) {
				if (type.id() == LogicalTypeId::DECIMAL) {
					fields.emplace_back(paimon::FieldType::DECIMAL, DecimalType::GetWidth(type),
					                    DecimalType::GetScale(type));
				} else {
					fields.emplace_back(PaimonTypeUtils::ConvertFieldType(type));
				}
			}
			return paimon::BucketIdCalculator::CreateHive(is_pk_table, num_buckets, fields, pool);
		}
		for (auto &type : column_types) {
			if (num_buckets > 1 && PaimonTypeUtils::ConvertFieldType(type) == paimon::FieldType::UNKNOWN) {
				throw NotImplementedException("Paimon default bucketing does not support bucket key type %s", type);
			}
		}
		return paimon::BucketIdCalculator::Create(is_pk_table, num_buckets, pool);
	};
	auto calculator = create();
	if (!calculator.ok()) {
		throw InvalidInputException("Invalid Paimon bucketing configuration: %s", calculator.status().ToString());
	}
	return std::move(calculator).value();
}

} // namespace duckdb
