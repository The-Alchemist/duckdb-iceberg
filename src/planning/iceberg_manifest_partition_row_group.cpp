#include "planning/iceberg_manifest_partition_row_group.hpp"

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "core/expression/iceberg_predicate_stats.hpp"

namespace duckdb {

namespace {

//! Types we can deserialize manifest lower/upper bounds for.
bool IsSupportedManifestAggregateType(const LogicalType &type) {
	if (type.id() == LogicalTypeId::INTERVAL) {
		// Interval bounds are not written reliably enough to fold aggregates on.
		return false;
	}
	return type.IsNumeric() || type.IsTemporal() || type.id() == LogicalTypeId::VARCHAR;
}

//! Whether manifest min/max can be treated as the true column extrema (required for constant folding).
bool ManifestMinMaxIsExact(const LogicalType &type) {
	if (type.id() == LogicalTypeId::VARCHAR || type.id() == LogicalTypeId::INTERVAL) {
		return false;
	}
	return type.IsNumeric() || type.IsTemporal();
}

//! Iceberg omits bounds for all-null columns; skipping them lets the optimizer scan that file instead.
bool ColumnHasNonNullValues(const IcebergDataFile &data_file, int32_t field_id) {
	auto null_it = data_file.null_value_counts.find(field_id);
	if (null_it == data_file.null_value_counts.end()) {
		return true;
	}
	if (null_it->second == 0) {
		return true;
	}
	return NumericCast<int64_t>(null_it->second) < data_file.record_count;
}

unique_ptr<BaseStatistics> BuildStatsFromManifestBounds(const LogicalType &type, const Value &lower,
                                                        const Value &upper) {
	if (type.id() == LogicalTypeId::VARCHAR) {
		auto stats = StringStats::CreateEmpty(type);
		// Iceberg may truncate string bounds to a prefix; mark inexact so TryExecuteAggregates falls back to scan.
		StringStats::SetMin(stats, lower.GetValueUnsafe<string_t>(), StringStatsType::TRUNCATED_STATS);
		StringStats::SetMax(stats, upper.GetValueUnsafe<string_t>(), StringStatsType::TRUNCATED_STATS);
		return stats.ToUnique();
	}
	auto stats = NumericStats::CreateEmpty(type);
	NumericStats::SetMin(stats, lower);
	NumericStats::SetMax(stats, upper);
	return stats.ToUnique();
}

} // namespace

IcebergManifestPartitionRowGroup::IcebergManifestPartitionRowGroup(unordered_map<idx_t, BaseStatistics> column_stats_p,
                                                                   unordered_map<idx_t, LogicalType> column_types_p)
    : column_stats(std::move(column_stats_p)), column_types(std::move(column_types_p)) {
}

void IcebergManifestPartitionRowGroup::AddFileStatistics(const IcebergDataFile &data_file,
                                                         const vector<unique_ptr<IcebergColumnDefinition>> &columns,
                                                         const IcebergTableMetadata &metadata,
                                                         optional_ptr<IcebergTableEntry> table,
                                                         vector<PartitionStatistics> &result) {
	PartitionStatistics partition_stats;
	partition_stats.count = NumericCast<idx_t>(data_file.record_count);
	partition_stats.count_type = CountType::COUNT_EXACT;

	// Only columns still present in the current schema mapping get stats (dropped fields are ignored).
	unordered_set<int32_t> mapping_field_ids;
	for (auto &mapping : metadata.mappings) {
		if (mapping.field_id != NumericLimits<int32_t>::Maximum()) {
			mapping_field_ids.insert(mapping.field_id);
		}
	}

	unordered_map<idx_t, BaseStatistics> stats_map;
	unordered_map<idx_t, LogicalType> type_map;
	for (idx_t col_idx = 0; col_idx < columns.size(); col_idx++) {
		auto &column = *columns[col_idx];
		if (!IsSupportedManifestAggregateType(column.type)) {
			continue;
		}
		auto field_id = column.id;
		if (!metadata.mappings.empty() && mapping_field_ids.find(field_id) == mapping_field_ids.end()) {
			continue;
		}
		if (!ColumnHasNonNullValues(data_file, field_id)) {
			continue;
		}

		auto lower_it = data_file.lower_bounds.find(field_id);
		auto upper_it = data_file.upper_bounds.find(field_id);
		// Missing bounds for one column (e.g. schema-evolved column on an old file) do not drop the file;
		// the optimizer will scan it for that aggregate while still folding other columns/partitions.
		if (lower_it == data_file.lower_bounds.end() || upper_it == data_file.upper_bounds.end()) {
			continue;
		}

		auto predicate_stats =
		    IcebergPredicateStats::DeserializeBounds(lower_it->second, upper_it->second, column.name, column.type);
		if (!predicate_stats.has_lower_bounds || !predicate_stats.has_upper_bounds ||
		    predicate_stats.lower_bound.IsNull() || predicate_stats.upper_bound.IsNull()) {
			continue;
		}

		auto column_stats =
		    BuildStatsFromManifestBounds(column.type, predicate_stats.lower_bound, predicate_stats.upper_bound);
		if (!column_stats) {
			continue;
		}

		StorageIndex storage_index;
		if (table) {
			storage_index = table->GetStorageIndex(ColumnIndex(col_idx));
		} else {
			storage_index = StorageIndex::FromColumnIndex(ColumnIndex(col_idx));
		}
		auto primary_index = storage_index.GetPrimaryIndex();
		stats_map.emplace(primary_index, std::move(*column_stats));
		type_map.emplace(primary_index, column.type);
	}

	if (!stats_map.empty()) {
		partition_stats.partition_row_group =
		    make_shared_ptr<IcebergManifestPartitionRowGroup>(std::move(stats_map), std::move(type_map));
	}
	// Always emit one PartitionStatistics per live data file so row counts stay aligned with scan indices.
	result.push_back(std::move(partition_stats));
}

unique_ptr<BaseStatistics> IcebergManifestPartitionRowGroup::GetColumnStatistics(const StorageIndex &storage_index) {
	auto it = column_stats.find(storage_index.GetPrimaryIndex());
	if (it == column_stats.end()) {
		return nullptr;
	}
	return it->second.ToUnique();
}

bool IcebergManifestPartitionRowGroup::MinMaxIsExact(const BaseStatistics &stats, const StorageIndex &storage_index) {
	// Exactness depends on the logical type, not the serialized stats payload (VARCHAR uses TRUNCATED_STATS).
	auto it = column_types.find(storage_index.GetPrimaryIndex());
	if (it == column_types.end()) {
		return false;
	}
	return ManifestMinMaxIsExact(it->second);
}

} // namespace duckdb
