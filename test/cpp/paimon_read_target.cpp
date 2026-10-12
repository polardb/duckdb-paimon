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

void CheckIdentifier(const paimon::Identifier &identifier, const std::string &table) {
	REQUIRE(identifier.GetDatabaseName() == "db");
	REQUIRE(identifier.GetTableName() == table);
}

void CheckLoads(const SpyCatalog &catalog, const std::vector<std::string> &tables) {
	REQUIRE(catalog.loads.size() == tables.size());
	for (size_t i = 0; i < tables.size(); ++i) {
		CAPTURE(i);
		CheckIdentifier(catalog.loads[i], tables[i]);
	}
}

void CheckTarget(const paimon::Result<PaimonReadTarget> &result, const std::string &branch,
                 const std::string &schema_table, const std::shared_ptr<paimon::Schema> &schema) {
	INFO(result.status().ToString());
	REQUIRE(result.ok());
	CheckIdentifier(result.value().base_identifier, "t");
	CheckIdentifier(result.value().schema_identifier, schema_table);
	REQUIRE(result.value().branch == branch);
	REQUIRE(result.value().schema == schema);
}

TEST_CASE("Paimon read target: branch precedence and exact schema lookups", "[paimon][read_target]") {
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
		SpyCatalog catalog;
		auto initial = DataSchema(test.schema_options);
		auto selected = DataSchema({{"branch", "must-not-redirect"}});
		catalog.responses = {initial, selected};
		auto result = PaimonReadTarget::Load(catalog, paimon::Identifier("db", test.table), test.overrides);
		CheckTarget(result, test.branch, test.loads.back(), test.loads.size() == 1 ? initial : selected);
		CheckLoads(catalog, test.loads);
	}
}

TEST_CASE("Paimon read target: blank values normalize to bare main", "[paimon][read_target]") {
	for (const std::string blank : {"", " ", "\t", "\n", "\r", "\f", "\v", " \t\n\r\f\v"}) {
		for (bool override_branch : {false, true}) {
			CAPTURE(blank, override_branch);
			SpyCatalog catalog;
			auto initial = DataSchema({{"branch", override_branch ? "dev" : blank}});
			catalog.responses = {initial};
			Options overrides = override_branch ? Options {{"branch", blank}} : Options {};
			CheckTarget(PaimonReadTarget::Load(catalog, paimon::Identifier("db", "t"), overrides), "main", "t",
			            initial);
			CheckLoads(catalog, {"t"});
		}
		if (!blank.empty()) {
			SpyCatalog catalog;
			auto initial = DataSchema({{"branch", "dev"}});
			catalog.responses = {initial};
			CheckTarget(PaimonReadTarget::Load(catalog, paimon::Identifier("db", "t$branch_" + blank), {}), "main", "t",
			            initial);
			CheckLoads(catalog, {"t"});
		}
	}
}

TEST_CASE("Paimon read target: nonblank branch bytes are preserved", "[paimon][read_target]") {
	for (const std::string branch : {" dev ", "\tdev\n", " main ", "MAIN", "Dev"}) {
		for (int source : {0, 1, 2}) {
			CAPTURE(branch, source);
			SpyCatalog catalog;
			auto initial = DataSchema(source == 0 ? Options {{"branch", branch}} : Options {});
			auto selected = DataSchema();
			catalog.responses = {initial, selected};
			const auto table = "t$branch_" + branch;
			auto result = PaimonReadTarget::Load(catalog, paimon::Identifier("db", source == 2 ? table : "t"),
			                                     source == 1 ? Options {{"branch", branch}} : Options {});
			CheckTarget(result, branch, table, source == 2 ? initial : selected);
			CheckLoads(catalog, source == 2 ? std::vector<std::string> {table} : std::vector<std::string> {"t", table});
		}
	}
}

TEST_CASE("Paimon read target: schema errors and null results propagate", "[paimon][read_target]") {
	const std::vector<SchemaResult> failures {paimon::Status::NotExist("missing branch schema"),
	                                          paimon::Status::IOError("catalog transport failed"),
	                                          std::shared_ptr<paimon::Schema> {}};
	for (bool redirected : {false, true}) {
		for (const auto &failure : failures) {
			CAPTURE(redirected, failure.status().ToString());
			SpyCatalog catalog;
			if (redirected) {
				catalog.responses.push_back(DataSchema({{"branch", "dev"}}));
			}
			catalog.responses.push_back(failure);
			auto result = PaimonReadTarget::Load(catalog, paimon::Identifier("db", "t"), {});
			REQUIRE_FALSE(result.ok());
			if (failure.ok()) {
				REQUIRE(result.status().IsInvalid());
				REQUIRE_THAT(result.status().ToString(), Catch::Contains("Paimon table schema is empty"));
			} else {
				REQUIRE(result.status().ToString() == failure.status().ToString());
				REQUIRE(result.status().IsNotExist() == failure.status().IsNotExist());
				REQUIRE(result.status().IsIOError() == failure.status().IsIOError());
			}
			CheckLoads(catalog,
			           redirected ? std::vector<std::string> {"t", "t$branch_dev"} : std::vector<std::string> {"t"});
		}
	}
}

TEST_CASE("Paimon read target: invalid identifiers never reach the catalog", "[paimon][read_target]") {
	for (const std::string table : {"", "t$branch_", "t$", "t$branch_dev$", "t$a$b$c"}) {
		CAPTURE(table);
		SpyCatalog catalog;
		auto result = PaimonReadTarget::Load(catalog, paimon::Identifier("db", table), {});
		REQUIRE_FALSE(result.ok());
		REQUIRE(result.status().IsInvalid());
		REQUIRE(catalog.loads.empty());
	}
}

TEST_CASE("Paimon read target: system schema identifiers remain intact", "[paimon][read_target]") {
	for (const std::string table : {"t$options", "t$branch_main$options", "t$branch_dev$options"}) {
		CAPTURE(table);
		SpyCatalog catalog;
		std::shared_ptr<paimon::Schema> schema = std::make_shared<StubSchema<paimon::SystemSchema>>();
		catalog.responses = {schema};
		auto result = PaimonReadTarget::Load(catalog, paimon::Identifier("db", table), {{"branch", "audit"}});
		CheckTarget(result, table == "t$branch_dev$options" ? "dev" : "main", table, schema);
		CheckLoads(catalog, {table});
	}
}

} // namespace
