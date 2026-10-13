// Copyright (c) 2026, Alibaba Group Holding Limited
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "catch.hpp"
#include "paimon_snapshot_resolver.hpp"
#include <cstdlib>

using duckdb::PaimonSnapshotResolver;
using Snapshots = std::vector<paimon::SnapshotInfo>;

static paimon::SnapshotInfo Snapshot(int64_t id, int64_t time, int64_t schema = 0) {
	paimon::SnapshotInfo info {};
	info.snapshot_id = id;
	info.time_millis = time;
	info.schema_id = schema;
	return info;
}

static std::optional<int64_t> Select(const Snapshots &snapshots, std::optional<int64_t> id = {},
                                     std::optional<int64_t> time = {}, int64_t schema = 0) {
	auto result = PaimonSnapshotResolver::Resolve(snapshots, id, time, schema);
	INFO(result.status().ToString());
	REQUIRE(result.ok());
	return result.value();
}

TEST_CASE("Paimon metadata: latest and explicit IDs", "[paimon][snapshot_metadata]") {
	Snapshots snapshots {Snapshot(3, 100), Snapshot(4, 200), Snapshot(6, 300)};
	REQUIRE(Select(snapshots) == 6);
	REQUIRE(Select(snapshots, 3) == 3);
	REQUIRE(Select(snapshots, 6) == 6);
	for (auto id : {0, 1, 5, 7}) {
		CAPTURE(id);
		REQUIRE(PaimonSnapshotResolver::Resolve(snapshots, id, {}, 0).status().IsNotExist());
	}
	REQUIRE_FALSE(Select({}).has_value());
	REQUIRE_FALSE(PaimonSnapshotResolver::Resolve({}, 1, {}, 0).ok());
	REQUIRE_FALSE(PaimonSnapshotResolver::Resolve({}, {}, 100, 0).ok());
	REQUIRE_FALSE(PaimonSnapshotResolver::Resolve(snapshots, 3, 100, 0).ok());
}

TEST_CASE("Paimon metadata: timestamp boundaries and equal times", "[paimon][snapshot_metadata]") {
	Snapshots snapshots {Snapshot(3, 100), Snapshot(4, 200), Snapshot(5, 200), Snapshot(6, 400)};
	REQUIRE_FALSE(PaimonSnapshotResolver::Resolve(snapshots, {}, 99, 0).ok());
	REQUIRE(Select(snapshots, {}, 100) == 3);
	REQUIRE(Select(snapshots, {}, 199) == 3);
	REQUIRE(Select(snapshots, {}, 200) == 5);
	REQUIRE(Select(snapshots, {}, 399) == 5);
	REQUIRE(Select(snapshots, {}, 400) == 6);
	REQUIRE(Select(snapshots, {}, std::numeric_limits<int64_t>::max()) == 6);
}

TEST_CASE("Paimon metadata: native numeric search and clock skew", "[paimon][snapshot_metadata]") {
	// A vector-index search would incorrectly skip the missing numeric probe 4.
	Snapshots gap {Snapshot(1, 10), Snapshot(2, 20), Snapshot(5, 50)};
	REQUIRE(PaimonSnapshotResolver::Resolve(gap, {}, 50, 0).status().IsNotExist());
	REQUIRE(Select(gap) == 5);
	REQUIRE(Select(gap, 5) == 5);
	// Native search does not choose the maximum matching ID for arbitrary skew.
	Snapshots skew {Snapshot(1, 10), Snapshot(2, 100), Snapshot(3, 20)};
	REQUIRE(Select(skew, {}, 30) == 1);
	Snapshots earliest_guard {Snapshot(1, 100), Snapshot(2, 10)};
	REQUIRE_FALSE(PaimonSnapshotResolver::Resolve(earliest_guard, {}, 50, 0).ok());
	auto max = std::numeric_limits<int64_t>::max();
	REQUIRE(Select({Snapshot(max, 10)}, {}, 10) == max);
}

TEST_CASE("Paimon metadata: schema and catalog contract guards", "[paimon][snapshot_metadata]") {
	Snapshots snapshots {Snapshot(1, 10, 0), Snapshot(2, 20, 2)};
	REQUIRE_FALSE(PaimonSnapshotResolver::Resolve(snapshots, {}, {}, 1).ok());
	REQUIRE_FALSE(PaimonSnapshotResolver::Resolve(snapshots, 2, {}, 1).ok());
	REQUIRE_FALSE(PaimonSnapshotResolver::Resolve(snapshots, {}, 20, 1).ok());
	REQUIRE(Select(snapshots, 1, {}, 1) == 1);
	REQUIRE(Select(snapshots, {}, {}, 2) == 2);
	REQUIRE_FALSE(PaimonSnapshotResolver::Resolve({Snapshot(2, 1), Snapshot(1, 2)}, {}, {}, 0).ok());
	REQUIRE_FALSE(PaimonSnapshotResolver::Resolve({Snapshot(1, 1), Snapshot(1, 2)}, {}, {}, 0).ok());
	REQUIRE_FALSE(PaimonSnapshotResolver::Resolve({Snapshot(0, 1)}, {}, {}, 0).ok());
	REQUIRE_FALSE(PaimonSnapshotResolver::Resolve({Snapshot(1, 1, -1)}, {}, {}, 0).ok());
}

TEST_CASE("Paimon metadata: bounded typed listing retry", "[paimon][snapshot_metadata]") {
	int calls = 0;
	auto recovered = PaimonSnapshotResolver::ListWithRetry([&]() -> paimon::Result<Snapshots> {
		if (++calls == 1) {
			return paimon::Status::NotExist("expired during listing");
		}
		return Snapshots {Snapshot(2, 20)};
	});
	REQUIRE(recovered.ok());
	REQUIRE(calls == 2);
	REQUIRE(Select(recovered.value()) == 2);
	calls = 0;
	auto exhausted = PaimonSnapshotResolver::ListWithRetry([&]() -> paimon::Result<Snapshots> {
		calls++;
		return paimon::Status::NotExist("expired again");
	});
	REQUIRE(exhausted.status().IsNotExist());
	REQUIRE(calls == 3);
	calls = 0;
	auto corrupt = PaimonSnapshotResolver::ListWithRetry([&]() -> paimon::Result<Snapshots> {
		calls++;
		return paimon::Status::IOError("Not exist text must not cause a retry");
	});
	REQUIRE(corrupt.status().IsIOError());
	REQUIRE(calls == 1);
	calls = 0;
	auto empty = PaimonSnapshotResolver::ListWithRetry([&]() -> paimon::Result<Snapshots> {
		calls++;
		return Snapshots {};
	});
	REQUIRE(empty.ok());
	REQUIRE(calls == 1);
	REQUIRE_FALSE(Select(empty.value()).has_value());
}

TEST_CASE("Paimon metadata: integer selector validation", "[paimon][snapshot_metadata]") {
	for (auto text : {"", "+1", " 1", "1x", "9223372036854775808", "-9223372036854775809"}) {
		CAPTURE(text);
		REQUIRE_FALSE(PaimonSnapshotResolver::ParseInteger(text, "selector").ok());
	}
	for (auto value :
	     {int64_t(0), int64_t(-1), std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()}) {
		auto parsed = PaimonSnapshotResolver::ParseInteger(std::to_string(value), "selector");
		REQUIRE(parsed.ok());
		REQUIRE(parsed.value() == value);
	}
}

struct LocalTimezone {
	std::optional<std::string> previous;
	explicit LocalTimezone(const char *zone) {
		if (auto value = std::getenv("TZ")) {
			previous = value;
		}
		setenv("TZ", zone, 1);
		tzset();
	}
	~LocalTimezone() {
		if (previous) {
			setenv("TZ", previous->c_str(), 1);
		} else {
			unsetenv("TZ");
		}
		tzset();
	}
};

TEST_CASE("Paimon metadata: native local timestamp formats", "[paimon][snapshot_metadata]") {
	const auto offset = GENERATE(0, 8);
	LocalTimezone timezone(offset == 0 ? "UTC0" : "CST-8");
	auto base = int64_t(86400000) - int64_t(offset) * 3600000;
	for (auto suffix : {"", " 00:00:00", " 00:00:00.0", " 00:00:00.000"}) {
		auto parsed = PaimonSnapshotResolver::ParseTimestamp(std::string("1970-01-02") + suffix);
		REQUIRE(parsed.ok());
		REQUIRE(parsed.value() == base);
	}
	for (auto fraction : {"1", "12", "123"}) {
		auto parsed = PaimonSnapshotResolver::ParseTimestamp(std::string("1970-01-02 00:00:00.") + fraction);
		REQUIRE(parsed.ok());
		auto expected = std::string(fraction) + std::string(3 - std::string(fraction).size(), '0');
		REQUIRE(parsed.value() == base + std::stoi(expected));
	}
	for (auto text : {"", "not a date", "2026-02-30", "2026-01-01junk", "2026-01-01 00:00:00.",
	                  "2026-01-01 00:00:00.1234", "2026-01-01 25:00:00", "2026-01-01 00:00:00Z"}) {
		CAPTURE(text);
		REQUIRE_FALSE(PaimonSnapshotResolver::ParseTimestamp(text).ok());
	}
	REQUIRE(PaimonSnapshotResolver::ParseTimestamp("2024-02-29").ok());
	REQUIRE_FALSE(PaimonSnapshotResolver::ParseTimestamp("2025-02-29").ok());
}
