/*
 * Copyright (c) 2026, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "duckdb/function/table_function.hpp"
#include "paimon_read_bridge.h"

namespace duckdb {

// Discovery uses the matching DuckDB C++ ABI. Callbacks and descriptors do not
// expose paimon-cpp, Arrow C++, STL objects or exceptions across the boundary.
struct PaimonReadBridgeInfo final : public TableFunctionInfo {
	explicit PaimonReadBridgeInfo(const PaimonReadBridgeV1 &api_p) : api(api_p) {
	}
	const PaimonReadBridgeV1 &api;
};

// trusted must be an unmodified definition from an independently authenticated
// provider, kept alive with its module. A catalog name alone is not sufficient.
// Authenticate before downcasting: hidden provider/consumer RTTI need not agree.
inline const PaimonReadBridgeV1 *TryGetPaimonReadBridge(const BaseTableFunction &candidate,
                                                        const BaseTableFunction &trusted) {
	if (!trusted.function || !trusted.bind || !trusted.init_global || !trusted.init_local || !trusted.function_info ||
	    candidate.function != trusted.function || candidate.bind != trusted.bind ||
	    candidate.init_global != trusted.init_global || candidate.init_local != trusted.init_local ||
	    candidate.function_info != trusted.function_info) {
		return nullptr;
	}
	auto &api = static_cast<const PaimonReadBridgeInfo &>(*candidate.function_info).api;
	if (api.magic != PAIMON_READ_BRIDGE_MAGIC ||
	    !((api.version == PAIMON_READ_BRIDGE_VERSION && api.struct_size == sizeof(PaimonReadBridgeV1)) ||
	      (api.version == PAIMON_READ_BRIDGE_NATIVE_VERSION && api.struct_size == sizeof(PaimonReadBridgeV2))) ||
	    !api.accepts_bind || !api.retain || !api.describe || !api.release) {
		return nullptr;
	}
	return &api;
}

inline const PaimonReadBridgeV2 *TryGetPaimonNativeReadBridge(const TableFunction &candidate,
                                                              const TableFunction &trusted) {
	auto *prefix = TryGetPaimonReadBridge(candidate, trusted);
	if (!prefix || prefix->version != PAIMON_READ_BRIDGE_NATIVE_VERSION) {
		return nullptr;
	}
	auto *api = reinterpret_cast<const PaimonReadBridgeV2 *>(prefix);
	if (!api->create_plan || !api->release_plan || !api->get_split || !api->release_split || !api->open_reader ||
	    !api->release_reader || !api->next_batch || !api->release_batch) {
		return nullptr;
	}
	return api;
}

} // namespace duckdb
