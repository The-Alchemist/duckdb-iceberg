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

#include "core/metadata/schema/iceberg_column_definition.hpp"

namespace duckdb {

struct IcebergDataFile;
class IcebergTableEntry;
class IcebergTableMetadata;

//! PartitionRowGroup implementation backed by Iceberg manifest lower/upper bounds for a single data file.
//! Enables DuckDB's eager MIN/MAX aggregate rewrite (StatisticsPropagator::TryExecuteAggregates) to fold bare
//! MIN/MAX expressions to constants without reading any Parquet column data.
class IcebergManifestPartitionRowGroup : public PartitionRowGroup {
public:
	explicit IcebergManifestPartitionRowGroup(unordered_map<idx_t, BaseStatistics> column_stats_p);

public:
	//! Build the per-column statistics for a single data file from its manifest bounds.
	//! Columns whose bounds are missing, all-null, of an unsupported type, or excluded by name-mapping rules are
	//! omitted from the result (causing the optimizer to fall back to a full scan).
	static shared_ptr<IcebergManifestPartitionRowGroup>
	Create(const vector<unique_ptr<IcebergColumnDefinition>> &columns, const IcebergTableMetadata &metadata,
	       const IcebergDataFile &data_file, optional_ptr<IcebergTableEntry> table);

public:
	unique_ptr<BaseStatistics> GetColumnStatistics(const StorageIndex &storage_index) override;
	bool MinMaxIsExact(const BaseStatistics &stats, const StorageIndex &storage_index) override;

private:
	//! Map from storage (primary) index -> manifest-derived column statistics
	unordered_map<idx_t, BaseStatistics> column_stats;
};

} // namespace duckdb
