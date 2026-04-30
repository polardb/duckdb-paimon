/*-------------------------------------------------------------------------
 *
 * paimon_scan.cpp
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
 *	  src/paimon_storage/paimon_scan.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "duckdb.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include "paimon_catalog.hpp"
#include "paimon_functions.hpp"
#include "paimon_type_utils.hpp"

#include "paimon/api.h"
#include "paimon/catalog/identifier.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/schema/schema.h"

#include <string>

namespace duckdb {

struct PaimonScanBindData : public TableFunctionData {
public:
	PaimonTablePath path;

	map<string, string> paimon_options;

	ArrowTableSchema arrow_table;

	std::shared_ptr<paimon::Predicate> predicates = nullptr;
};

static std::shared_ptr<paimon::Predicate> TryConvertComparison(const BoundComparisonExpression &comp, LogicalGet &get) {
	// early exit: only handle comparison types supported by paimon-cpp
	switch (comp.type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		break;
	default:
		return nullptr;
	}

	// normalize to col OP constant
	Expression *col_expr = nullptr;
	Expression *const_expr = nullptr;
	ExpressionType comparison_type = comp.type;

	if (comp.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
	    comp.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		col_expr = comp.left.get();
		const_expr = comp.right.get();
	} else if (comp.left->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
	           comp.right->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		col_expr = comp.right.get();
		const_expr = comp.left.get();
		comparison_type = FlipComparisonExpression(comp.type);
	} else {
		return nullptr;
	}

	auto filter_binding_idx = col_expr->Cast<BoundColumnRefExpression>().binding.column_index;
	auto col_idx = get.GetColumnIds()[filter_binding_idx];

	auto val = const_expr->Cast<BoundConstantExpression>().value;
	auto paimon_type = PaimonTypeUtils::ConvertFieldType(get.GetColumnType(col_idx));
	auto literal = PaimonTypeUtils::ConvertLiteral(val, paimon_type);
	if (!literal) {
		return nullptr;
	}

	auto field_index = col_idx.GetPrimaryIndex();
	auto &field_name = get.GetColumnName(col_idx);

	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return paimon::PredicateBuilder::Equal(field_index, field_name, paimon_type, literal.value());
	case ExpressionType::COMPARE_NOTEQUAL:
		return paimon::PredicateBuilder::NotEqual(field_index, field_name, paimon_type, literal.value());
	case ExpressionType::COMPARE_LESSTHAN:
		return paimon::PredicateBuilder::LessThan(field_index, field_name, paimon_type, literal.value());
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return paimon::PredicateBuilder::LessOrEqual(field_index, field_name, paimon_type, literal.value());
	case ExpressionType::COMPARE_GREATERTHAN:
		return paimon::PredicateBuilder::GreaterThan(field_index, field_name, paimon_type, literal.value());
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return paimon::PredicateBuilder::GreaterOrEqual(field_index, field_name, paimon_type, literal.value());
	default:
		return nullptr;
	}
}

static std::shared_ptr<paimon::Predicate> TryConvertBetween(const BoundBetweenExpression &between, LogicalGet &get) {
	if (between.input->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
	    between.lower->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT ||
	    between.upper->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		return nullptr;
	}

	auto filter_binding_idx = between.input->Cast<BoundColumnRefExpression>().binding.column_index;
	auto col_idx = get.GetColumnIds()[filter_binding_idx];
	auto paimon_type = PaimonTypeUtils::ConvertFieldType(get.GetColumnType(col_idx));

	auto lower_literal =
	    PaimonTypeUtils::ConvertLiteral(between.lower->Cast<BoundConstantExpression>().value, paimon_type);
	auto upper_literal =
	    PaimonTypeUtils::ConvertLiteral(between.upper->Cast<BoundConstantExpression>().value, paimon_type);
	if (!lower_literal || !upper_literal) {
		return nullptr;
	}

	auto field_index = col_idx.GetPrimaryIndex();
	auto &field_name = get.GetColumnName(col_idx);

	// Decompose into two comparison predicates combined with AND.  paimon-cpp's
	// Between() is itself just GreaterOrEqual + LessOrEqual + And, so there is no
	// benefit in special-casing inclusive bounds — this unified path handles every
	// combination (inclusive, exclusive, or mixed) produced by the optimizer.
	auto lower_pred =
	    between.lower_inclusive
	        ? paimon::PredicateBuilder::GreaterOrEqual(field_index, field_name, paimon_type, lower_literal.value())
	        : paimon::PredicateBuilder::GreaterThan(field_index, field_name, paimon_type, lower_literal.value());
	auto upper_pred =
	    between.upper_inclusive
	        ? paimon::PredicateBuilder::LessOrEqual(field_index, field_name, paimon_type, upper_literal.value())
	        : paimon::PredicateBuilder::LessThan(field_index, field_name, paimon_type, upper_literal.value());
	auto result = paimon::PredicateBuilder::And({lower_pred, upper_pred});
	return result.ok() ? std::move(result.value()) : nullptr;
}

// Forward declaration for mutual recursion with TryConvertOperator.
static std::shared_ptr<paimon::Predicate> TryConvertExpression(const Expression &expr, LogicalGet &get);

static std::shared_ptr<paimon::Predicate> TryConvertOperator(const BoundOperatorExpression &op, LogicalGet &get) {
	// Validate children count per operator type.
	switch (op.type) {
	case ExpressionType::COMPARE_IN:
	case ExpressionType::COMPARE_NOT_IN:
		D_ASSERT(op.children.size() >= 2);
		break;
	case ExpressionType::OPERATOR_IS_NULL:
	case ExpressionType::OPERATOR_IS_NOT_NULL:
	case ExpressionType::OPERATOR_NOT:
		D_ASSERT(op.children.size() == 1);
		break;
	default:
		return nullptr;
	}

	// NOT wraps an arbitrary sub-expression; handle it before the column-ref gate.
	if (op.type == ExpressionType::OPERATOR_NOT) {
		auto child_pred = TryConvertExpression(*op.children[0], get);
		if (!child_pred) {
			return nullptr;
		}
		auto result = paimon::PredicateBuilder::Not(child_pred);
		return result.ok() ? std::move(result.value()) : nullptr;
	}

	// From here on, the first child must be a column reference.
	if (op.children[0]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return nullptr;
	}

	// Get column index and name.
	auto filter_binding_idx = op.children[0]->Cast<BoundColumnRefExpression>().binding.column_index;
	auto col_idx = get.GetColumnIds()[filter_binding_idx];
	auto paimon_type = PaimonTypeUtils::ConvertFieldType(get.GetColumnType(col_idx));
	auto field_index = col_idx.GetPrimaryIndex();
	auto &field_name = get.GetColumnName(col_idx);

	switch (op.type) {
	case ExpressionType::OPERATOR_IS_NULL:
		return paimon::PredicateBuilder::IsNull(field_index, field_name, paimon_type);
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		return paimon::PredicateBuilder::IsNotNull(field_index, field_name, paimon_type);
	case ExpressionType::COMPARE_IN:
	case ExpressionType::COMPARE_NOT_IN: {
		// Collect literals from children[1..n].
		std::vector<paimon::Literal> literals;
		for (idx_t i = 1; i < op.children.size(); i++) {
			if (op.children[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
				// Best effort pushdown.
				if (op.type == ExpressionType::COMPARE_NOT_IN) {
					continue;
				} else {
					return nullptr;
				}
			}

			auto val = op.children[i]->Cast<BoundConstantExpression>().value;
			auto literal = PaimonTypeUtils::ConvertLiteral(val, paimon_type);
			if (!literal) {
				// Same reason as above: best effort pushdown.
				if (op.type == ExpressionType::COMPARE_NOT_IN) {
					continue;
				} else {
					return nullptr;
				}
			}
			literals.push_back(std::move(literal.value()));
		}

		if (literals.empty()) {
			return nullptr;
		}

		if (op.type == ExpressionType::COMPARE_IN) {
			return paimon::PredicateBuilder::In(field_index, field_name, paimon_type, literals);
		} else {
			return paimon::PredicateBuilder::NotIn(field_index, field_name, paimon_type, literals);
		}
	}
	default:
		return nullptr;
	}
}

static std::shared_ptr<paimon::Predicate> TryConvertConjunction(const BoundConjunctionExpression &conj,
                                                                LogicalGet &get) {
	std::vector<std::shared_ptr<paimon::Predicate>> predicates;

	for (auto &child : conj.children) {
		auto pred = TryConvertExpression(*child, get);
		if (pred) {
			predicates.push_back(std::move(pred));
			continue;
		}

		D_ASSERT(!pred);

		// For AND: skip unconvertible children (they stay in DuckDB's filter).
		// For OR: the entire OR must be convertible, otherwise give up.
		if (conj.type == ExpressionType::CONJUNCTION_OR) {
			return nullptr;
		}
	}

	// No predicate parsed.
	if (predicates.empty()) {
		return nullptr;
	}

	if (conj.type == ExpressionType::CONJUNCTION_AND) {
		auto result = paimon::PredicateBuilder::And(predicates);
		return result.ok() ? std::move(result.value()) : nullptr;
	} else if (conj.type == ExpressionType::CONJUNCTION_OR) {
		auto result = paimon::PredicateBuilder::Or(predicates);
		return result.ok() ? std::move(result.value()) : nullptr;
	}

	return nullptr;
}

// Supported function types for predicate pushdown.
// DuckDB's LikeOptimizationRule rewrites LIKE into optimized functions:
//   LIKE 'abc%'  → prefix(col, 'abc')   → StartsWith
//   LIKE '%abc'  → suffix(col, 'abc')   → EndsWith
//   LIKE '%abc%' → contains(col, 'abc') → Contains
// The optimizer strips '%' from the pattern, matching paimon-cpp's expectation.
// Unrewritten LIKE ('~~') keeps the full pattern (with % and _).
enum class PushdownFunction : uint8_t { PREFIX, SUFFIX, CONTAINS, LIKE, UNSUPPORTED };

static PushdownFunction ClassifyFunction(const string &name) {
	if (name == "prefix") {
		return PushdownFunction::PREFIX;
	} else if (name == "suffix") {
		return PushdownFunction::SUFFIX;
	} else if (name == "contains") {
		return PushdownFunction::CONTAINS;
	} else if (name == "~~") {
		return PushdownFunction::LIKE;
	}
	return PushdownFunction::UNSUPPORTED;
}

static std::shared_ptr<paimon::Predicate> TryConvertFunction(const BoundFunctionExpression &func, LogicalGet &get) {
	// Gate: reject unsupported functions early.
	auto func_type = ClassifyFunction(func.function.name);
	if (func_type == PushdownFunction::UNSUPPORTED) {
		return nullptr;
	}

	// All supported functions need at least a column ref as the first argument.
	D_ASSERT(func.children.size() >= 1);
	if (func.children[0]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return nullptr;
	}

	// Extract column metadata.
	auto filter_binding_idx = func.children[0]->Cast<BoundColumnRefExpression>().binding.column_index;
	auto col_idx = get.GetColumnIds()[filter_binding_idx];
	auto field_index = col_idx.GetPrimaryIndex();
	auto &field_name = get.GetColumnName(col_idx);
	auto paimon_type = PaimonTypeUtils::ConvertFieldType(get.GetColumnType(col_idx));

	// Per-category validation and argument extraction.
	std::optional<paimon::Literal> pattern_literal;

	switch (func_type) {
	case PushdownFunction::PREFIX:
	case PushdownFunction::SUFFIX:
	case PushdownFunction::CONTAINS:
	case PushdownFunction::LIKE: {
		// LIKE-family: require STRING field and a constant pattern argument.
		if (paimon_type != paimon::FieldType::STRING) {
			return nullptr;
		}
		D_ASSERT(func.children.size() == 2);
		if (func.children[1]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
			return nullptr;
		}

		auto &pattern_value = func.children[1]->Cast<BoundConstantExpression>().value;
		pattern_literal = PaimonTypeUtils::ConvertLiteral(pattern_value, paimon_type);
		if (!pattern_literal) {
			return nullptr;
		}
		break;
	}
	default:
		return nullptr;
	}

	// Dispatch to the corresponding paimon-cpp predicate builder.
	switch (func_type) {
	case PushdownFunction::PREFIX: {
		auto result =
		    paimon::PredicateBuilder::StartsWith(field_index, field_name, paimon_type, pattern_literal.value());
		return result.ok() ? std::move(result.value()) : nullptr;
	}
	case PushdownFunction::SUFFIX: {
		auto result = paimon::PredicateBuilder::EndsWith(field_index, field_name, paimon_type, pattern_literal.value());
		return result.ok() ? std::move(result.value()) : nullptr;
	}
	case PushdownFunction::CONTAINS: {
		auto result = paimon::PredicateBuilder::Contains(field_index, field_name, paimon_type, pattern_literal.value());
		return result.ok() ? std::move(result.value()) : nullptr;
	}
	case PushdownFunction::LIKE: {
		auto result = paimon::PredicateBuilder::Like(field_index, field_name, paimon_type, pattern_literal.value());
		return result.ok() ? std::move(result.value()) : nullptr;
	}
	default:
		return nullptr;
	}
}

static std::shared_ptr<paimon::Predicate> TryConvertExpression(const Expression &expr, LogicalGet &get) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COMPARISON:
		return TryConvertComparison(expr.Cast<BoundComparisonExpression>(), get);
	case ExpressionClass::BOUND_CONJUNCTION:
		return TryConvertConjunction(expr.Cast<BoundConjunctionExpression>(), get);
	case ExpressionClass::BOUND_BETWEEN:
		return TryConvertBetween(expr.Cast<BoundBetweenExpression>(), get);
	case ExpressionClass::BOUND_OPERATOR:
		return TryConvertOperator(expr.Cast<BoundOperatorExpression>(), get);
	case ExpressionClass::BOUND_FUNCTION:
		return TryConvertFunction(expr.Cast<BoundFunctionExpression>(), get);
	default:
		return nullptr;
	}
}

static void PaimonPushdownFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data,
                                 vector<unique_ptr<Expression>> &filters) {
	auto &bind = bind_data->Cast<PaimonScanBindData>();
	std::vector<std::shared_ptr<paimon::Predicate>> predicates;

	for (idx_t i = 0; i < filters.size(); i++) {
		auto pred = TryConvertExpression(*filters[i], get);
		if (pred) {
			predicates.push_back(std::move(pred));
		}

		// We do not remove the filter from DuckDB's filter list here.
		// Although the predicate has been pushed down to paimon-cpp,
		// paimon-cpp performs predicate pushdown on a best-effort basis,
		// which means it may return more rows than expected.
		// Therefore, DuckDB must retain the filter in its execution plan
		// to ensure correctness by re-evaluating the predicate.
	}

	if (!predicates.empty()) {
		auto top_predicate = paimon::PredicateBuilder::And(predicates);
		if (!top_predicate.ok()) {
			throw IOException(top_predicate.status().ToString());
		}
		bind.predicates = std::move(top_predicate.value());
	} else {
		bind.predicates = nullptr;
	}
}

static unique_ptr<FunctionData> PaimonScanBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<PaimonScanBindData>();

	auto path = PaimonTablePath::Parse(input.inputs);
	bind_data->path = path;

	unordered_map<string, Value> scan_options(input.named_parameters.begin(), input.named_parameters.end());
	bind_data->paimon_options = PaimonCatalog::GetPaimonOptions(context, path.warehouse, scan_options);
	auto paimon_catalog = PaimonCatalog::CreatePaimonCatalog(context, path.warehouse, scan_options);

	auto table_schema_result = paimon_catalog->LoadTableSchema(paimon::Identifier(path.dbname, path.tablename));
	if (!table_schema_result.ok()) {
		throw IOException(table_schema_result.status().ToString());
	}
	auto table_schema = std::move(table_schema_result).value();

	auto arrow_schema_result = table_schema->GetArrowSchema();
	if (!arrow_schema_result.ok()) {
		throw IOException(arrow_schema_result.status().ToString());
	}
	auto arrow_schema = std::move(arrow_schema_result).value();

	// return types and cols
	ArrowTableSchema arrow_table;
	ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table, *arrow_schema);

	names = arrow_table.GetNames();
	return_types = arrow_table.GetTypes();

	bind_data->arrow_table = std::move(arrow_table);

	if (arrow_schema && arrow_schema->release) {
		arrow_schema->release(arrow_schema.get());
	}

	return std::move(bind_data);
}

struct PaimonScanGlobalState : public GlobalTableFunctionState {
	std::vector<std::shared_ptr<paimon::Split>> splits;
	atomic<idx_t> split_index {0};
	string path;
	ArrowTableSchema arrow_table;
	std::shared_ptr<paimon::Predicate> paimon_predicates = nullptr;

	idx_t MaxThreads() const override {
		return splits.size();
	}
};

struct PaimonScanLocalState : public LocalTableFunctionState {
public:
	shared_ptr<ArrowArrayWrapper> chunk;
	std::vector<int> read_column_ids;

	PaimonScanLocalState(PaimonScanGlobalState &gstate, const PaimonScanBindData &bind_data,
	                     vector<column_t> column_ids)
	    : global_state(gstate), bind_data(bind_data) {
		read_column_ids.reserve(column_ids.size());
		auto column_count = bind_data.arrow_table.GetColumns().size();
		for (auto column_id : column_ids) {
			if (IsRowIdColumnId(column_id)) {
				throw InvalidInputException("Paimon tables do not support selecting rowid");
			}
			if (column_id >= column_count) {
				throw InvalidInputException("Invalid projected column id %llu for paimon scan",
				                            NumericCast<unsigned long long>(column_id));
			}
			read_column_ids.push_back(NumericCast<int>(column_id));
		}
		NextSplit();
	}

	~PaimonScanLocalState() override {
		ReleaseCurrentBatch();
	}

	paimon::BatchReader::ReadBatch NextBatch() {
		while (!exhausted) {
			auto batch_result = batch_reader->NextBatch();
			if (!batch_result.ok()) {
				throw IOException(batch_result.status().ToString());
			}
			auto batch = std::move(batch_result).value();
			ReleaseCurrentBatch();

			// current split exhausted, try to grab the next one
			if (paimon::BatchReader::IsEofBatch(batch)) {
				// no more splits
				if (!NextSplit()) {
					return paimon::BatchReader::MakeEofBatch();
				}

				continue;
			}

			return batch;
		}

		return paimon::BatchReader::MakeEofBatch();
	}

private:
	unique_ptr<paimon::BatchReader> batch_reader;
	bool exhausted = false;
	PaimonScanGlobalState &global_state;
	const PaimonScanBindData &bind_data;

	//! Release the current batch's arrow data. Must be called before
	//! overwriting arrow_array with a new batch (to avoid leaking the
	//! old one) and in the destructor (before batch_reader is destroyed,
	//! since the release callback may depend on paimon resources).
	void ReleaseCurrentBatch() {
		if (chunk && chunk->arrow_array.release) {
			chunk->arrow_array.release(&chunk->arrow_array);
			chunk->arrow_array.release = nullptr;
		}
	}

	bool NextSplit() {
		// grab the next split from global state and create a new batch_reader
		auto idx = global_state.split_index.fetch_add(1);

		// no more splits are available
		if (idx >= global_state.splits.size()) {
			exhausted = true;
			return false;
		}

		paimon::ReadContextBuilder read_context_builder(global_state.path);
		auto read_context_result = read_context_builder.SetOptions(bind_data.paimon_options)
		                               .SetReadFieldIds(read_column_ids)
		                               .SetPredicate(global_state.paimon_predicates)
		                               .EnablePredicateFilter(false)
		                               .Finish();
		if (!read_context_result.ok()) {
			throw IOException(read_context_result.status().ToString());
		}
		auto read_context = std::move(read_context_result).value();

		auto table_read_result = paimon::TableRead::Create(std::move(read_context));
		if (!table_read_result.ok()) {
			throw IOException(table_read_result.status().ToString());
		}
		auto table_read = std::move(table_read_result).value();

		auto batch_reader_result = table_read->CreateReader(global_state.splits[idx]);
		if (!batch_reader_result.ok()) {
			throw IOException(batch_reader_result.status().ToString());
		}
		batch_reader.reset(std::move(batch_reader_result).value().release());

		return true;
	}
};

static unique_ptr<GlobalTableFunctionState> PaimonScanInitGlobal(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<PaimonScanBindData>();
	auto state = make_uniq<PaimonScanGlobalState>();

	// Remap predicates from table schema to read schema.
	// The original predicate tree was constructed against the full table schema,
	// but DuckDB may have already pruned columns via projection pushdown,
	// resulting in a narrower read schema. We need to remap the column indices
	// in the predicate tree to match the read schema so that paimon-cpp can
	// correctly apply predicate pushdown on the projected columns.
	if (bind.predicates != nullptr) {
		auto &col_names = const_cast<ArrowTableSchema &>(bind.arrow_table).GetNames();
		std::map<std::string, int32_t> column_ids_mapping;
		for (idx_t i = 0; i < input.column_ids.size(); i++) {
			column_ids_mapping.emplace(col_names[input.column_ids[i]], i);
		}

		auto pred_res = paimon::PredicateUtils::CreatePickedFieldFilter(bind.predicates, column_ids_mapping);
		if (!pred_res.ok()) {
			throw IOException(pred_res.status().ToString());
		}

		state->paimon_predicates = pred_res.value();
	}

	auto path = bind.path.warehouse + "/" + bind.path.dbname + paimon::Catalog::DB_SUFFIX + "/" + bind.path.tablename;

	// construct the scanner
	paimon::ScanContextBuilder scan_context_builder(path);

	auto scan_context_result = scan_context_builder.SetOptions(bind.paimon_options).Finish();
	if (!scan_context_result.ok()) {
		throw IOException(scan_context_result.status().ToString());
	}
	auto scan_context = std::move(scan_context_result).value();

	auto scanner_result = paimon::TableScan::Create(std::move(scan_context));
	if (!scanner_result.ok()) {
		throw IOException(scanner_result.status().ToString());
	}
	auto scanner = std::move(scanner_result).value();

	auto plan_result = scanner->CreatePlan();
	if (!plan_result.ok()) {
		throw IOException(plan_result.status().ToString());
	}
	auto plan = std::move(plan_result).value();

	state->splits = plan->Splits();
	state->path = path;
	state->arrow_table = bind.arrow_table;

	return std::move(state);
}

static unique_ptr<LocalTableFunctionState> PaimonScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *gstate) {
	auto &global_state = gstate->Cast<PaimonScanGlobalState>();
	auto &bind = input.bind_data->Cast<PaimonScanBindData>();
	auto local_state = make_uniq<PaimonScanLocalState>(global_state, bind, input.column_ids);

	return std::move(local_state);
}

static void PaimonScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &global_state = input.global_state->Cast<PaimonScanGlobalState>();
	auto &local_state = input.local_state->Cast<PaimonScanLocalState>();
	auto current_chunk = make_shared_ptr<ArrowArrayWrapper>();

	auto batch = local_state.NextBatch();
	if (paimon::BatchReader::IsEofBatch(batch)) {
		return;
	}

	auto &[c_array, c_schema] = batch;
	if (c_schema && c_schema->release) {
		c_schema->release(c_schema.get());
	}

	current_chunk->arrow_array = *c_array;
	c_array->release = nullptr;
	local_state.chunk = current_chunk;

	auto output_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE, NumericCast<idx_t>(c_array->length));
	output.SetCardinality(output_size);

	auto &arrow_types = global_state.arrow_table.GetColumns();

	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
		// the first column is ignored
		auto &child_array = *c_array->children[col_idx + 1];
		auto &arrow_type = *arrow_types.at(local_state.read_column_ids[col_idx]);

		ArrowArrayScanState array_state(context);
		array_state.owned_data = current_chunk;

		ArrowToDuckDBConversion::SetValidityMask(output.data[col_idx], child_array, 0, output_size,
		                                         current_chunk->arrow_array.offset, -1);
		ArrowToDuckDBConversion::ColumnArrowToDuckDB(output.data[col_idx], child_array, 0, array_state, output_size,
		                                             arrow_type);
	}

	output.Verify();
	return;
}

TableFunctionSet PaimonFunctions::GetPaimonScanFunction() {
	TableFunctionSet function_set("paimon_scan");

	auto fun = TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, PaimonScan,
	                         PaimonScanBind, PaimonScanInitGlobal);
	fun.named_parameters["manifest_format"] = LogicalType::VARCHAR; // deprecated: auto-detected from table schema
	fun.named_parameters["file_format"] = LogicalType::VARCHAR;     // deprecated: auto-detected from table schema
	fun.named_parameters["snapshot_from_id"] = LogicalType::BIGINT;
	fun.named_parameters["snapshot_from_timestamp"] = LogicalType::TIMESTAMP;
	fun.init_local = PaimonScanInitLocal;
	fun.projection_pushdown = true;
	fun.pushdown_complex_filter = PaimonPushdownFilter;
	function_set.AddFunction(fun);

	auto fun_fullpath = TableFunction({LogicalType::VARCHAR}, PaimonScan, PaimonScanBind, PaimonScanInitGlobal);
	fun_fullpath.named_parameters["manifest_format"] = LogicalType::VARCHAR; // deprecated
	fun_fullpath.named_parameters["file_format"] = LogicalType::VARCHAR;     // deprecated
	fun_fullpath.named_parameters["snapshot_from_id"] = LogicalType::BIGINT;
	fun_fullpath.named_parameters["snapshot_from_timestamp"] = LogicalType::TIMESTAMP;
	fun_fullpath.init_local = PaimonScanInitLocal;
	fun_fullpath.projection_pushdown = true;
	fun_fullpath.pushdown_complex_filter = PaimonPushdownFilter;
	function_set.AddFunction(fun_fullpath);

	return function_set;
}

} // namespace duckdb
