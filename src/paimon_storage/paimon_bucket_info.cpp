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
#include "paimon/defs.h"

#include <limits>

namespace duckdb {

PaimonBucketInfo PaimonBucketInfo::Bind(const vector<string> &names, const vector<LogicalType> &types,
                                        const vector<string> &partition_keys, const std::vector<string> &primary_keys,
                                        const std::map<string, string> &options) {
	PaimonBucketInfo result;
	result.is_pk_table = !primary_keys.empty();
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

} // namespace duckdb
