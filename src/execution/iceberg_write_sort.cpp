#include "execution/iceberg_write_sort.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include "core/expression/iceberg_transform.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"

namespace duckdb {

namespace {

static vector<idx_t> GetColumnPath(const ColumnIndex &column_index) {
	vector<idx_t> path;
	path.reserve(column_index.ChildIndexCount());
	for (auto &child_index : column_index.GetChildIndexes()) {
		path.push_back(child_index.GetPrimaryIndex());
	}
	return path;
}

static ColumnIndex GetColumnIndexBySourceId(const IcebergTableSchema &schema, idx_t source_id) {
	auto column_index = schema.TryGetColumnIndexByFieldId(source_id);
	if (!column_index) {
		throw InvalidInputException("Partition/sort source column with id %d not found in schema", source_id);
	}
	return *column_index;
}

static unique_ptr<Expression> CreateSourceColumnReference(ClientContext &context, const IcebergCopyInput &copy_input,
                                                          uint64_t source_id) {
	auto column_index = GetColumnIndexBySourceId(copy_input.schema, source_id);
	auto primary_index = column_index.GetPrimaryIndex();
	auto &root_column = *copy_input.schema.columns[primary_index];
	auto result = IcebergWriteSort::CreateColumnReference(copy_input, root_column.type, primary_index);
	for (auto &child_index : GetColumnPath(column_index)) {
		vector<unique_ptr<Expression>> children;
		children.push_back(std::move(result));
		children.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(NumericCast<int64_t>(child_index + 1))));

		ErrorData error;
		FunctionBinder binder(context);
		result = binder.BindScalarFunction(Identifier::DefaultSchema(), Identifier("struct_extract_at"),
		                                   std::move(children), error, false);
		if (!result) {
			error.Throw();
		}
	}
	return result;
}

static unique_ptr<Expression> BindTransformFunction(ClientContext &context, const string &name,
                                                    vector<unique_ptr<Expression>> children) {
	ErrorData error;
	FunctionBinder binder(context);
	auto function =
	    binder.BindScalarFunction(Identifier::DefaultSchema(), Identifier(name), std::move(children), error, false);
	if (!function) {
		error.Throw();
	}
	return function;
}

//! Iceberg partition/sort transforms for year/month/day/hour are defined as:
//! - years: date_diff('year', DATE '1970-01-01', source_column)
//! - months: date_diff('month', DATE '1970-01-01', source_column)
//! - days: date_diff('day', DATE '1970-01-01', source_column)
//! - hours: date_diff('hour', TIMESTAMP '1970-01-01', source_column)
static unique_ptr<Expression> GetDateDiffFunction(ClientContext &context, const IcebergCopyInput &copy_input,
                                                  const string &date_part, uint64_t source_id) {
	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundConstantExpression>(Value(date_part)));
	if (date_part == "hour") {
		children.push_back(make_uniq<BoundConstantExpression>(Value::TIMESTAMP(Timestamp::FromEpochSeconds(0))));
	} else {
		children.push_back(make_uniq<BoundConstantExpression>(Value::DATE(Date::FromDate(1970, 1, 1))));
	}
	children.push_back(CreateSourceColumnReference(context, copy_input, source_id));
	return BindTransformFunction(context, "date_diff", std::move(children));
}

static unique_ptr<Expression> GetBucketExpression(ClientContext &context, const IcebergCopyInput &copy_input,
                                                  uint64_t source_id, const IcebergTransform &transform) {
	vector<unique_ptr<Expression>> children;
	children.push_back(
	    make_uniq<BoundConstantExpression>(Value::INTEGER(static_cast<int32_t>(transform.GetBucketModulo()))));
	children.push_back(CreateSourceColumnReference(context, copy_input, source_id));
	return BindTransformFunction(context, "iceberg_bucket", std::move(children));
}

static unique_ptr<Expression> GetTruncateExpression(ClientContext &context, const IcebergCopyInput &copy_input,
                                                    uint64_t source_id, const IcebergTransform &transform) {
	vector<unique_ptr<Expression>> children;
	children.push_back(
	    make_uniq<BoundConstantExpression>(Value::INTEGER(static_cast<int32_t>(transform.GetTruncateWidth()))));
	children.push_back(CreateSourceColumnReference(context, copy_input, source_id));
	return BindTransformFunction(context, "iceberg_truncate", std::move(children));
}

static OrderType GetDuckDBOrderType(const string &direction) {
	if (StringUtil::CIEquals(direction, "asc")) {
		return OrderType::ASCENDING;
	}
	if (StringUtil::CIEquals(direction, "desc")) {
		return OrderType::DESCENDING;
	}
	throw NotImplementedException("Unsupported Iceberg sort direction '%s'", direction);
}

static OrderByNullType GetDuckDBNullOrder(const string &null_order) {
	if (StringUtil::CIEquals(null_order, "nulls-first")) {
		return OrderByNullType::NULLS_FIRST;
	}
	if (StringUtil::CIEquals(null_order, "nulls-last")) {
		return OrderByNullType::NULLS_LAST;
	}
	throw NotImplementedException("Unsupported Iceberg null order '%s'", null_order);
}

} // namespace

unique_ptr<Expression> IcebergWriteSort::CreateColumnReference(const IcebergCopyInput &copy_input,
                                                               const LogicalType &type, idx_t column_index) {
	if (copy_input.get_table_index.IsValid()) {
		ColumnBinding column_binding(TableIndex(copy_input.get_table_index.GetIndex()), ProjectionIndex(column_index));
		return make_uniq<BoundColumnRefExpression>(type, column_binding);
	}
	return make_uniq<BoundReferenceExpression>(type, column_index);
}

unique_ptr<Expression> IcebergWriteSort::GetTransformExpression(ClientContext &context,
                                                                const IcebergCopyInput &copy_input, uint64_t source_id,
                                                                const IcebergTransform &transform, const char *usage) {
	switch (transform.Type()) {
	case IcebergTransformType::IDENTITY:
		return CreateSourceColumnReference(context, copy_input, source_id);
	case IcebergTransformType::YEAR:
		return GetDateDiffFunction(context, copy_input, "year", source_id);
	case IcebergTransformType::MONTH:
		return GetDateDiffFunction(context, copy_input, "month", source_id);
	case IcebergTransformType::DAY:
		return GetDateDiffFunction(context, copy_input, "day", source_id);
	case IcebergTransformType::HOUR:
		return GetDateDiffFunction(context, copy_input, "hour", source_id);
	case IcebergTransformType::BUCKET:
		return GetBucketExpression(context, copy_input, source_id, transform);
	case IcebergTransformType::TRUNCATE:
		return GetTruncateExpression(context, copy_input, source_id, transform);
	case IcebergTransformType::VOID:
		throw InvalidInputException("VOID partition transform should not be used for %s", usage);
	default:
		throw NotImplementedException("Unsupported %s transform type", usage);
	}
}

vector<BoundOrderByNode> IcebergWriteSort::GenerateSortOrderExpressions(ClientContext &context,
                                                                        const IcebergCopyInput &copy_input) {
	vector<BoundOrderByNode> result;
	if (!copy_input.table_metadata.HasSortOrder()) {
		return result;
	}
	auto &sort_order = copy_input.table_metadata.GetLatestSortOrder();
	if (!sort_order.IsSorted()) {
		return result;
	}
	for (auto &field : sort_order.fields) {
		auto expr = GetTransformExpression(context, copy_input, field.source_id, field.transform, "sorting");
		result.emplace_back(GetDuckDBOrderType(field.direction), GetDuckDBNullOrder(field.null_order), std::move(expr));
	}
	return result;
}

void IcebergWriteSort::PopulateCopyOrderColumns(ClientContext &context, const IcebergCopyInput &copy_input,
                                                IcebergCopyOptions &result) {
	result.order_columns = GenerateSortOrderExpressions(context, copy_input);
}

void IcebergWriteSort::GeneratePhysicalOrder(PhysicalPlanGenerator &planner, vector<BoundOrderByNode> &orders,
                                             optional_ptr<PhysicalOperator> &plan) {
	D_ASSERT(plan);
	vector<idx_t> projections;
	projections.reserve(plan->GetTypes().size());
	for (idx_t i = 0; i < plan->GetTypes().size(); i++) {
		projections.push_back(i);
	}
	auto &order = planner.Make<PhysicalOrder>(plan->GetTypes(), std::move(orders), std::move(projections),
	                                          plan->estimated_cardinality);
	order.children.push_back(*plan);
	plan = order;
}

} // namespace duckdb
