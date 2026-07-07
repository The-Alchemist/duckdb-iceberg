//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planning/iceberg_manifest_partition_row_group.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/schema/iceberg_column_definition.hpp"

namespace duckdb {

class IcebergTableEntry;

//! Per-file partition stats derived from Iceberg manifest lower_bounds / upper_bounds.
//! Used by TryExecuteAggregates to fold bare MIN/MAX/COUNT(*) or to drive partial precompute
//! when only some files/columns have usable bounds.
class IcebergManifestPartitionRowGroup : public PartitionRowGroup {
public:
	explicit IcebergManifestPartitionRowGroup(unordered_map<idx_t, BaseStatistics> column_stats_p,
	                                          unordered_map<idx_t, LogicalType> column_types_p);

public:
	static void AddFileStatistics(const IcebergDataFile &data_file,
	                              const vector<unique_ptr<IcebergColumnDefinition>> &columns,
	                              const IcebergTableMetadata &metadata, optional_ptr<IcebergTableEntry> table,
	                              vector<PartitionStatistics> &result);

public:
	unique_ptr<BaseStatistics> GetColumnStatistics(const StorageIndex &storage_index) override;
	bool MinMaxIsExact(const BaseStatistics &stats, const StorageIndex &storage_index) override;

private:
	unordered_map<idx_t, BaseStatistics> column_stats;
	unordered_map<idx_t, LogicalType> column_types;
};

} // namespace duckdb
