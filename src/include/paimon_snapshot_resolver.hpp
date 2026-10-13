// Copyright (c) 2026, Alibaba Group Holding Limited
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "paimon/result.h"
#include "paimon/snapshot/snapshot_info.h"

#include <algorithm>
#include <charconv>
#include <cctype>
#include <ctime>
#include <iomanip>
#include <limits>
#include <sstream>
#include <vector>

namespace duckdb {

struct PaimonSnapshotResolver {
	// Expiration can remove an old file between listing and loading metadata.
	// A failed listing must never be interpreted as a successful empty listing.
	template <class ListSnapshots>
	static auto ListWithRetry(ListSnapshots list) -> decltype(list()) {
		for (int attempt = 0;; attempt++) {
			auto result = list();
			if (result.ok() || !result.status().IsNotExist() || attempt == 2) {
				return result;
			}
		}
	}

	static paimon::Result<int64_t> ParseInteger(const std::string &text, const std::string &option) {
		int64_t value;
		auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
		if (parsed.ec != std::errc() || parsed.ptr != text.data() + text.size()) {
			return paimon::Status::Invalid("Invalid Paimon ", option, ": expected a signed 64-bit integer");
		}
		return value;
	}

	// Adapted from Apache Paimon's StringUtils::StringToTimestampMillis at
	// 53f9c86d (Apache-2.0). It is not exposed by the public native API.
	// Preserve local-time, date-only and fractional-second behavior.
	static paimon::Result<int64_t> ParseTimestamp(const std::string &text) {
		std::tm parts {};
		parts.tm_isdst = -1;
		std::istringstream input(text);
		input >> std::get_time(&parts, "%Y-%m-%d %H:%M:%S");
		int64_t fraction = 0;
		if (!input.fail()) {
			if (input.peek() == '.') {
				input.get();
				int digits = 0;
				while (digits < 3 && input.peek() != std::char_traits<char>::eof() &&
				       std::isdigit(static_cast<unsigned char>(input.peek()))) {
					fraction = fraction * 10 + (input.get() - '0');
					digits++;
				}
				if (!digits) {
					return paimon::Status::Invalid("Invalid Paimon scan.timestamp: expected fractional digits");
				}
				for (; digits < 3; digits++) {
					fraction *= 10;
				}
			}
		} else {
			input.clear();
			input.str(text);
			parts = std::tm {};
			parts.tm_isdst = -1;
			input >> std::get_time(&parts, "%Y-%m-%d");
			if (input.fail()) {
				return paimon::Status::Invalid("Invalid Paimon scan.timestamp: expected date or local timestamp");
			}
		}
		if (input.peek() != std::char_traits<char>::eof()) {
			return paimon::Status::Invalid("Invalid Paimon scan.timestamp: unexpected trailing characters");
		}
		auto month = parts.tm_mon;
		auto day = parts.tm_mday;
		auto seconds = std::mktime(&parts);
		if (seconds == -1 || parts.tm_mon != month || parts.tm_mday != day ||
		    seconds > (std::numeric_limits<int64_t>::max() - fraction) / 1000 ||
		    seconds < std::numeric_limits<int64_t>::min() / 1000) {
			return paimon::Status::Invalid("Invalid Paimon scan.timestamp: invalid or out-of-range date");
		}
		return static_cast<int64_t>(seconds) * 1000 + fraction;
	}

	static paimon::Result<std::optional<int64_t>> Resolve(const std::vector<paimon::SnapshotInfo> &snapshots,
	                                                      std::optional<int64_t> requested_id,
	                                                      std::optional<int64_t> timestamp, int64_t read_schema_id) {
		if (requested_id && timestamp) {
			return paimon::Status::Invalid("Paimon scan cannot combine snapshot and timestamp selectors");
		}
		int64_t previous = 0;
		for (const auto &snapshot : snapshots) {
			if (snapshot.snapshot_id <= previous || snapshot.schema_id < 0) {
				return paimon::Status::Invalid("Invalid Paimon snapshot metadata ordering or schema ID");
			}
			previous = snapshot.snapshot_id;
		}
		if (snapshots.empty()) {
			if (requested_id || timestamp) {
				return paimon::Status::Invalid("Paimon requested version did not resolve to a snapshot");
			}
			return std::optional<int64_t>();
		}
		auto find = [&](int64_t id) -> const paimon::SnapshotInfo * {
			auto entry = std::lower_bound(
			    snapshots.begin(), snapshots.end(), id,
			    [](const paimon::SnapshotInfo &snapshot, int64_t value) { return snapshot.snapshot_id < value; });
			return entry == snapshots.end() || entry->snapshot_id != id ? nullptr : &*entry;
		};
		const paimon::SnapshotInfo *selected = &snapshots.back();
		if (requested_id) {
			selected = find(*requested_id);
			if (!selected) {
				return paimon::Status::NotExist("Paimon requested snapshot does not exist: ", *requested_id);
			}
		} else if (timestamp) {
			selected = nullptr;
			// Match SnapshotManager::FindSnapshotBeforeTimestamp, including its
			// earliest guard, numeric-ID probes and errors for missing probed IDs.
			if (snapshots.front().time_millis <= *timestamp) {
				int64_t low = snapshots.front().snapshot_id;
				int64_t high = snapshots.back().snapshot_id;
				while (low <= high) {
					auto mid = low + (high - low) / 2;
					auto snapshot = find(mid);
					if (!snapshot) {
						return paimon::Status::NotExist("Paimon timestamp lookup requires missing snapshot: ", mid);
					}
					if (snapshot->time_millis <= *timestamp) {
						selected = snapshot;
						if (mid == high) {
							break;
						}
						low = mid + 1;
					} else {
						high = mid - 1;
					}
				}
			}
			if (!selected) {
				return paimon::Status::Invalid(
				    "Paimon has no snapshot earlier than or equal to the requested timestamp");
			}
		}
		if (selected->schema_id > read_schema_id) {
			return paimon::Status::Invalid("Paimon snapshot schema is newer than the bound schema; retry the query");
		}
		return std::optional<int64_t>(selected->snapshot_id);
	}
};

} // namespace duckdb
