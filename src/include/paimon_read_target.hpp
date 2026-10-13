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

#include "paimon/catalog/catalog.h"
#include "paimon/schema/schema.h"
#include <algorithm>
#include <cctype>

namespace duckdb {

struct PaimonReadTarget {
	paimon::Identifier base_identifier;
	paimon::Identifier schema_identifier;
	std::string branch;
	std::shared_ptr<paimon::Schema> schema;

	static bool IsBlank(const std::string &value) {
		return std::all_of(value.begin(), value.end(), [](unsigned char c) { return std::isspace(c); });
	}

	static std::string NormalizeBranch(const std::string &value) {
		return IsBlank(value) ? paimon::Identifier::kDefaultMainBranch : value;
	}

	static paimon::Identifier SchemaIdentifier(const paimon::Identifier &base, const std::string &branch) {
		return paimon::Identifier(base.GetDatabaseName(), branch == paimon::Identifier::kDefaultMainBranch
		                                                      ? base.GetTableName()
		                                                      : base.GetTableName() + "$branch_" + branch);
	}

	static std::string ResolveBranch(const std::optional<std::string> &explicit_branch,
	                                 const std::map<std::string, std::string> &schema_options,
	                                 const std::map<std::string, std::string> &overrides) {
		if (explicit_branch) {
			return NormalizeBranch(*explicit_branch);
		}
		auto option = overrides.find("branch");
		if (option != overrides.end()) {
			return NormalizeBranch(option->second);
		}
		option = schema_options.find("branch");
		return NormalizeBranch(option == schema_options.end() ? "" : option->second);
	}

	// Resolve once from the requested identifier and its options. Options in the
	// selected branch's schema must not redirect the read to another branch.
	static paimon::Result<PaimonReadTarget> Load(paimon::Catalog &catalog, const paimon::Identifier &identifier,
	                                             const std::map<std::string, std::string> &overrides) {
		auto name = identifier.GetDataTableName();
		if (!name.ok()) {
			return name.status();
		}
		auto explicit_branch = identifier.GetBranchName();
		if (!explicit_branch.ok()) {
			return explicit_branch.status();
		}
		auto system = identifier.IsSystemTable();
		if (!system.ok()) {
			return system.status();
		}
		paimon::Identifier base(identifier.GetDatabaseName(), name.value());
		auto branch = NormalizeBranch(explicit_branch.value().value_or(paimon::Identifier::kDefaultMainBranch));
		auto lookup = system.value() ? identifier : SchemaIdentifier(base, branch);
		auto schema = catalog.LoadTableSchema(lookup);
		if (!schema.ok()) {
			return schema.status();
		}
		if (!schema.value()) {
			return paimon::Status::Invalid("Paimon table schema is empty");
		}
		auto data_schema = std::dynamic_pointer_cast<paimon::DataSchema>(schema.value());
		if (data_schema) {
			auto selected = ResolveBranch(explicit_branch.value(), data_schema->Options(), overrides);
			if (selected != branch) {
				branch = std::move(selected);
				schema = catalog.LoadTableSchema(SchemaIdentifier(base, branch));
				if (!schema.ok()) {
					return schema.status();
				}
				if (!schema.value()) {
					return paimon::Status::Invalid("Paimon table schema is empty");
				}
			}
		}
		auto resolved_identifier = system.value() ? identifier : SchemaIdentifier(base, branch);
		return PaimonReadTarget {std::move(base), std::move(resolved_identifier), std::move(branch),
		                         std::move(schema).value()};
	}
};

} // namespace duckdb
