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

#pragma once

#include "duckdb/common/types.hpp"

#include <map>

namespace duckdb {

struct PaimonBucketInfo {
	static PaimonBucketInfo Bind(const vector<string> &names, const vector<LogicalType> &types,
	                             const vector<string> &partition_keys, const std::vector<string> &primary_keys,
	                             const std::map<string, string> &options);

	void CheckWriteSupported() const;

	bool is_pk_table = false;
	int32_t num_buckets = -1;
};

} // namespace duckdb
