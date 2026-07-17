//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/iceberg_write_sort.hpp
//
// Shared Iceberg partition/sort transform expression helpers used by INSERT
// and iceberg_rewrite_data_files.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"

#include "execution/operator/iceberg_insert.hpp"

namespace duckdb {

struct IcebergWriteSort {
	//! Build BoundOrderByNode list from the table's default sort order.
	//! Returns empty when the table has no (non-empty) sort order.
	static vector<BoundOrderByNode> GenerateSortOrderExpressions(ClientContext &context,
	                                                             const IcebergCopyInput &copy_input);

	//! Populate IcebergCopyOptions.order_columns from the table sort order.
	static void PopulateCopyOrderColumns(ClientContext &context, const IcebergCopyInput &copy_input,
	                                     IcebergCopyOptions &result);

	//! Iceberg partition/sort transforms (identity, year/month/day/hour, bucket, truncate).
	static unique_ptr<Expression> GetTransformExpression(ClientContext &context, const IcebergCopyInput &copy_input,
	                                                     uint64_t source_id, const IcebergTransform &transform,
	                                                     const char *usage);

	//! Push a PhysicalOrder on top of plan (used for unpartitioned sorted writes).
	static void GeneratePhysicalOrder(PhysicalPlanGenerator &planner, vector<BoundOrderByNode> &orders,
	                                  optional_ptr<PhysicalOperator> &plan);

	static unique_ptr<Expression> CreateColumnReference(const IcebergCopyInput &copy_input, const LogicalType &type,
	                                                    idx_t column_index);
};

} // namespace duckdb
