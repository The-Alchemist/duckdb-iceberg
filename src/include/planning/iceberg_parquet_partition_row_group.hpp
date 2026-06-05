//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planning/iceberg_parquet_partition_row_group.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

#include "core/metadata/schema/iceberg_column_definition.hpp"

namespace duckdb {

class ClientContext;
struct OpenFileInfo;
class IcebergTableEntry;

//! PartitionRowGroup implementation backed by the Parquet footer statistics of a single data file.
//! Unlike the manifest-bounds path, the min/max here are gated on the Parquet `is_min_value_exact` /
//! `is_max_value_exact` flags, so DuckDB's eager MIN/MAX aggregate rewrite only fires when the bounds are
//! provably exact (correct even for truncated string bounds). No Parquet column *data* is read - only the footer.
class IcebergParquetPartitionRowGroup : public PartitionRowGroup {
public:
	explicit IcebergParquetPartitionRowGroup(unordered_map<idx_t, BaseStatistics> column_stats_p);

public:
	//! Read the Parquet footer for `file`, and append one PartitionStatistics for it to `result` (count from the
	//! footer, plus exact per-column statistics keyed by the storage index the optimizer will use). Columns are
	//! matched to Parquet columns by Iceberg field id; columns without exact footer bounds, of an unsupported type,
	//! or that can't be safely located are omitted (so the optimizer falls back to a full scan for them).
	static void AddFileStatistics(ClientContext &context, const OpenFileInfo &file, idx_t record_count,
	                              const vector<unique_ptr<IcebergColumnDefinition>> &columns,
	                              optional_ptr<IcebergTableEntry> table, vector<PartitionStatistics> &result);

public:
	unique_ptr<BaseStatistics> GetColumnStatistics(const StorageIndex &storage_index) override;
	bool MinMaxIsExact(const BaseStatistics &stats, const StorageIndex &storage_index) override;

private:
	//! Map from storage (primary) index -> exact Parquet-footer-derived column statistics
	unordered_map<idx_t, BaseStatistics> column_stats;
};

} // namespace duckdb
