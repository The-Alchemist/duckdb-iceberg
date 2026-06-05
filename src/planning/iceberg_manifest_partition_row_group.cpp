#include "planning/iceberg_manifest_partition_row_group.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "core/expression/iceberg_predicate_stats.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"

namespace duckdb {

namespace {

//! Iceberg stores exact min/max for numeric and temporal types. INTERVAL is temporal but is not backed by a numeric
//! physical type (so NumericStats can't represent it), hence it is excluded.
bool IsSupportedNumericType(const LogicalType &type) {
	if (type.id() == LogicalTypeId::INTERVAL) {
		return false;
	}
	return type.IsNumeric() || type.IsTemporal();
}

//! Build manifest-derived statistics for a single column, or return nullptr if no usable bounds exist.
unique_ptr<BaseStatistics> BuildColumnStatistics(const IcebergColumnDefinition &column,
                                                 const IcebergDataFile &data_file) {
	auto &type = column.type;
	const bool is_varchar = type.id() == LogicalTypeId::VARCHAR;
	if (!IsSupportedNumericType(type) && !is_varchar) {
		//! Unsupported type for eager MIN/MAX, fall back to a full scan.
		return nullptr;
	}

	auto lower_bound_it = data_file.lower_bounds.find(column.id);
	auto upper_bound_it = data_file.upper_bounds.find(column.id);
	if (lower_bound_it == data_file.lower_bounds.end() || upper_bound_it == data_file.upper_bounds.end()) {
		//! Need both bounds to answer both MIN and MAX.
		return nullptr;
	}

	auto stats =
	    IcebergPredicateStats::DeserializeBounds(lower_bound_it->second, upper_bound_it->second, column.name, type);
	if (!stats.has_lower_bounds || !stats.has_upper_bounds || stats.lower_bound.IsNull() ||
	    stats.upper_bound.IsNull()) {
		//! Missing or all-null bounds, can't use them.
		return nullptr;
	}

	if (is_varchar) {
		auto base_stats = StringStats::CreateEmpty(type);
		auto lower_string = StringValue::Get(stats.lower_bound);
		auto upper_string = StringValue::Get(stats.upper_bound);
		StringStats::Update(base_stats, string_t(lower_string));
		StringStats::Update(base_stats, string_t(upper_string));
		return base_stats.ToUnique();
	}

	auto base_stats = NumericStats::CreateEmpty(type);
	NumericStats::SetMin(base_stats, stats.lower_bound);
	NumericStats::SetMax(base_stats, stats.upper_bound);
	if (!NumericStats::HasMinMax(base_stats)) {
		return nullptr;
	}
	return base_stats.ToUnique();
}

} // namespace

IcebergManifestPartitionRowGroup::IcebergManifestPartitionRowGroup(unordered_map<idx_t, BaseStatistics> column_stats_p)
    : column_stats(std::move(column_stats_p)) {
}

shared_ptr<IcebergManifestPartitionRowGroup>
IcebergManifestPartitionRowGroup::Create(const vector<unique_ptr<IcebergColumnDefinition>> &columns,
                                         const IcebergTableMetadata &metadata, const IcebergDataFile &data_file,
                                         optional_ptr<IcebergTableEntry> table) {
	//! Name-mapping conservatism: if the metadata defines a name-mapping that doesn't reference a column's field id,
	//! we must assume that column is unreachable (entirely NULL), voiding the manifest bounds for it.
	unordered_set<int32_t> mapping_field_ids;
	for (auto &mapping : metadata.mappings) {
		if (mapping.field_id != NumericLimits<int32_t>::Maximum()) {
			mapping_field_ids.insert(mapping.field_id);
		}
	}

	unordered_map<idx_t, BaseStatistics> column_stats;
	for (idx_t col_idx = 0; col_idx < columns.size(); col_idx++) {
		auto &column = *columns[col_idx];

		if (!metadata.mappings.empty() && mapping_field_ids.find(column.id) == mapping_field_ids.end()) {
			//! The name-mapping is present but doesn't contain this field, omit it.
			continue;
		}

		auto built_stats = BuildColumnStatistics(column, data_file);
		if (!built_stats) {
			continue;
		}

		StorageIndex storage_index;
		if (table) {
			storage_index = table->GetStorageIndex(ColumnIndex(col_idx));
		} else {
			storage_index = StorageIndex::FromColumnIndex(ColumnIndex(col_idx));
		}
		column_stats.emplace(storage_index.GetPrimaryIndex(), std::move(*built_stats));
	}

	return make_shared_ptr<IcebergManifestPartitionRowGroup>(std::move(column_stats));
}

unique_ptr<BaseStatistics> IcebergManifestPartitionRowGroup::GetColumnStatistics(const StorageIndex &storage_index) {
	auto it = column_stats.find(storage_index.GetPrimaryIndex());
	if (it == column_stats.end()) {
		return nullptr;
	}
	return it->second.ToUnique();
}

bool IcebergManifestPartitionRowGroup::MinMaxIsExact(const BaseStatistics &stats, const StorageIndex &storage_index) {
	//! We only store statistics for columns that have valid manifest file-level bounds. Per the Iceberg spec these
	//! bounds are exact for the numeric and temporal types we support here.
	return column_stats.find(storage_index.GetPrimaryIndex()) != column_stats.end();
}

} // namespace duckdb
