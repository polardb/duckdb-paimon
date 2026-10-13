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
#include "paimon_read_target.hpp"

#include <stdexcept>

namespace {

using duckdb::PaimonReadTarget;
using Options = std::map<std::string, std::string>;
using SchemaResult = paimon::Result<std::shared_ptr<paimon::Schema>>;

[[noreturn]] void UnexpectedCall() {
	throw std::logic_error("Unexpected API call while resolving Paimon read target");
}

template <class Base>
class StubSchema : public Base {
public:
	paimon::Result<std::unique_ptr<ArrowSchema>> GetArrowSchema() const override {
		UnexpectedCall();
	}
	paimon::Result<std::string> GetJsonSchema() const override {
		UnexpectedCall();
	}
	std::vector<std::string> FieldNames() const override {
		UnexpectedCall();
	}
	paimon::Result<paimon::FieldType> GetFieldType(const std::string &) const override {
		UnexpectedCall();
	}
	std::optional<std::string> Comment() const override {
		UnexpectedCall();
	}
};

class StubDataSchema : public StubSchema<paimon::DataSchema> {
public:
	explicit StubDataSchema(std::map<std::string, std::string> options) : options(std::move(options)) {
	}
	int64_t Id() const override {
		UnexpectedCall();
	}
	const std::vector<std::string> &PrimaryKeys() const override {
		UnexpectedCall();
	}
	const std::vector<std::string> &PartitionKeys() const override {
		UnexpectedCall();
	}
	const std::vector<std::string> &BucketKeys() const override {
		UnexpectedCall();
	}
	int32_t NumBuckets() const override {
		UnexpectedCall();
	}
	int32_t HighestFieldId() const override {
		UnexpectedCall();
	}
	const std::map<std::string, std::string> &Options() const override {
		return options;
	}

private:
	const std::map<std::string, std::string> options;
};

std::shared_ptr<paimon::Schema> DataSchema(Options options = {}) {
	return std::make_shared<StubDataSchema>(std::move(options));
}

class SpyCatalog : public paimon::Catalog {
public:
	std::vector<SchemaResult> responses;
	mutable std::vector<paimon::Identifier> loads;

	SchemaResult LoadTableSchema(const paimon::Identifier &identifier) const override {
		loads.push_back(identifier);
		REQUIRE(loads.size() <= responses.size());
		return responses[loads.size() - 1];
	}

	paimon::Status CreateDatabase(const std::string &, const Options &, bool) override {
		UnexpectedCall();
	}
	paimon::Status CreateTable(const paimon::Identifier &, ArrowSchema *, const std::vector<std::string> &,
	                           const std::vector<std::string> &, const Options &, bool) override {
		UnexpectedCall();
	}
	paimon::Result<std::vector<std::string>> ListDatabases() const override {
		UnexpectedCall();
	}
	paimon::Result<std::vector<std::string>> ListTables(const std::string &) const override {
		UnexpectedCall();
	}
	paimon::Status DropDatabase(const std::string &, bool, bool) override {
		UnexpectedCall();
	}
	paimon::Status DropTable(const paimon::Identifier &, bool) override {
		UnexpectedCall();
	}
	paimon::Status RenameTable(const paimon::Identifier &, const paimon::Identifier &, bool) override {
		UnexpectedCall();
	}
	paimon::Result<std::shared_ptr<paimon::Table>> GetTable(const paimon::Identifier &) const override {
		UnexpectedCall();
	}
	paimon::Result<bool> DatabaseExists(const std::string &) const override {
		UnexpectedCall();
	}
	paimon::Result<bool> TableExists(const paimon::Identifier &) const override {
		UnexpectedCall();
	}
	std::string GetDatabaseLocation(const std::string &) const override {
		UnexpectedCall();
	}
	paimon::Result<std::string> GetTableLocation(const paimon::Identifier &) const override {
		UnexpectedCall();
	}
	std::string GetRootPath() const override {
		UnexpectedCall();
	}
	std::shared_ptr<paimon::FileSystem> GetFileSystem() const override {
		UnexpectedCall();
	}
	const Options &GetOptions() const override {
		UnexpectedCall();
	}
	paimon::Result<std::vector<paimon::SnapshotInfo>> ListSnapshots(const paimon::Identifier &,
	                                                                const std::string &) const override {
		UnexpectedCall();
	}
};

void CheckBranch(const paimon::Identifier &identifier, const Options &schema_options, const Options &overrides,
                 const std::string &expected, const std::vector<std::string> &expected_loads) {
	SpyCatalog catalog;
	auto initial = DataSchema(schema_options);
	// A schema loaded from the selected branch must not select a third branch.
	auto selected = DataSchema({{"branch", "must-not-redirect"}});
	catalog.responses = {initial, selected};
	auto result = PaimonReadTarget::Load(catalog, identifier, overrides);
	INFO(result.status().ToString());
	REQUIRE(result.ok());
	REQUIRE(result.value().base_identifier.GetDatabaseName() == "db");
	REQUIRE(result.value().base_identifier.GetTableName() == "t");
	REQUIRE(result.value().branch == expected);
	REQUIRE(result.value().schema_identifier.GetDatabaseName() == "db");
	REQUIRE(result.value().schema_identifier.GetTableName() == expected_loads.back());
	REQUIRE(result.value().schema == (expected_loads.size() == 1 ? initial : selected));
	std::vector<std::string> loads;
	for (const auto &load : catalog.loads) {
		REQUIRE(load.GetDatabaseName() == "db");
		loads.push_back(load.GetTableName());
	}
	REQUIRE(loads == expected_loads);
}

TEST_CASE("Paimon read target: branch precedence and schema loading", "[paimon][read_target]") {
	struct Case {
		std::string table;
		Options schema_options;
		Options overrides;
		std::string branch;
		std::vector<std::string> loads;
	};
	const std::vector<Case> cases {
	    {"t", {}, {}, "main", {"t"}},
	    {"t", {{"branch", "main"}}, {}, "main", {"t"}},
	    {"t", {{"branch", "dev"}}, {}, "dev", {"t", "t$branch_dev"}},
	    {"t", {}, {{"branch", "dev"}}, "dev", {"t", "t$branch_dev"}},
	    {"t", {{"branch", "dev"}}, {{"branch", "audit"}}, "audit", {"t", "t$branch_audit"}},
	    {"t", {{"branch", "dev"}}, {{"branch", "main"}}, "main", {"t"}},
	    {"t$branch_main", {{"branch", "dev"}}, {{"branch", "audit"}}, "main", {"t"}},
	    {"t$branch_dev", {{"branch", "audit"}}, {{"branch", "main"}}, "dev", {"t$branch_dev"}},
	    {"t$branch_dev", {}, {{"branch", ""}}, "dev", {"t$branch_dev"}}};
	for (const auto &test : cases) {
		CAPTURE(test.table, test.branch, test.schema_options, test.overrides);
		CheckBranch(paimon::Identifier("db", test.table), test.schema_options, test.overrides, test.branch, test.loads);
	}
}

TEST_CASE("Paimon read target: blank values normalize to bare main", "[paimon][read_target]") {
	for (const std::string blank : {"", " ", "\t", "\n", "\r", "\f", "\v", " \t\n\r\f\v"}) {
		for (bool override_branch : {false, true}) {
			CAPTURE(blank, override_branch);
			Options schema_options {{"branch", override_branch ? "dev" : blank}};
			Options overrides = override_branch ? Options {{"branch", blank}} : Options {};
			CheckBranch(paimon::Identifier("db", "t"), schema_options, overrides, "main", {"t"});
		}
		if (!blank.empty()) {
			CheckBranch(paimon::Identifier("db", "t$branch_" + blank), {{"branch", "dev"}}, {}, "main", {"t"});
		}
	}
}

TEST_CASE("Paimon read target: nonblank branch bytes are preserved", "[paimon][read_target]") {
	for (const std::string branch : {" dev ", "\tdev\n", " main ", "MAIN", "Dev"}) {
		for (int source : {0, 1, 2}) {
			CAPTURE(branch, source);
			Options schema_options = source == 0 ? Options {{"branch", branch}} : Options {};
			Options overrides = source == 1 ? Options {{"branch", branch}} : Options {};
			const auto table = "t$branch_" + branch;
			CheckBranch(paimon::Identifier("db", source == 2 ? table : "t"), schema_options, overrides, branch,
			            source == 2 ? std::vector<std::string> {table} : std::vector<std::string> {"t", table});
		}
	}
}

} // namespace
