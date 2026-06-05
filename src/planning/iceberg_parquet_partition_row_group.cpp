#include "planning/iceberg_parquet_partition_row_group.hpp"

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/types.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"

#include "parquet_reader.hpp"
#include "parquet_column_schema.hpp"

namespace duckdb {

namespace {

//! Eager MIN/MAX folding only supports numeric, temporal (excluding INTERVAL) and varchar columns - matching the
//! comparators DuckDB's StatisticsPropagator::TryExecuteAggregates is able to evaluate.
bool IsSupportedType(const LogicalType &type) {
	if (type.id() == LogicalTypeId::VARCHAR) {
		return true;
	}
	if (type.id() == LogicalTypeId::INTERVAL) {
		return false;
	}
	return type.IsNumeric() || type.IsTemporal();
}

//! Returns the Parquet column chunk's exact-min-and-max flag for the given row group.
bool ColumnChunkMinMaxIsExact(const duckdb_parquet::FileMetaData &file_meta, idx_t row_group_idx, idx_t column_index) {
	auto &row_group = file_meta.row_groups[row_group_idx];
	if (column_index >= row_group.columns.size()) {
		return false;
	}
	auto &column_chunk = row_group.columns[column_index];
	if (!column_chunk.__isset.meta_data || !column_chunk.meta_data.__isset.statistics) {
		return false;
	}
	auto &stats = column_chunk.meta_data.statistics;
	if (!stats.__isset.is_min_value_exact || !stats.__isset.is_max_value_exact) {
		return false;
	}
	return stats.is_min_value_exact && stats.is_max_value_exact;
}

//! Build a map from Iceberg field id -> top-level Parquet column index, restricted to flat (leaf) columns whose
//! Parquet column-chunk index matches their position. This keeps the exact-flag lookup (which is positional on the
//! row group's column chunks) safe in the presence of nested columns.
unordered_map<int32_t, idx_t> BuildFieldIdToParquetColumn(const duckdb_parquet::FileMetaData &file_meta,
                                                          const ParquetColumnSchema &root_schema) {
	unordered_map<int32_t, idx_t> result;
	for (idx_t j = 0; j < root_schema.children.size(); j++) {
		auto &child = root_schema.children[j];
		if (!child.children.empty()) {
			//! Nested column - leaf-positional chunk lookup isn't safe, skip.
			continue;
		}
		if (!child.schema_index.IsValid() || child.column_index != j) {
			continue;
		}
		auto &schema_element = file_meta.schema[child.schema_index.GetIndex()];
		if (!schema_element.__isset.field_id) {
			continue;
		}
		result[schema_element.field_id] = j;
	}
	return result;
}

} // namespace

IcebergParquetPartitionRowGroup::IcebergParquetPartitionRowGroup(unordered_map<idx_t, BaseStatistics> column_stats_p)
    : column_stats(std::move(column_stats_p)) {
}

void IcebergParquetPartitionRowGroup::AddFileStatistics(ClientContext &context, const OpenFileInfo &file,
                                                        idx_t record_count,
                                                        const vector<unique_ptr<IcebergColumnDefinition>> &columns,
                                                        optional_ptr<IcebergTableEntry> table,
                                                        vector<PartitionStatistics> &result) {
	PartitionStatistics partition_stats;
	partition_stats.count = record_count;
	partition_stats.count_type = CountType::COUNT_EXACT;

	unique_ptr<ParquetReader> reader;
	try {
		reader = make_uniq<ParquetReader>(context, file, ParquetOptions(context), nullptr);
	} catch (...) {
		//! Could not read the Parquet footer - emit a count-only partition (keeps COUNT(*) folding working) without
		//! row-group stats, which makes the optimizer fall back to a full scan for MIN/MAX.
		result.push_back(std::move(partition_stats));
		return;
	}

	auto file_meta = reader->GetFileMetadata();
	auto &root_schema = *reader->root_schema;
	auto field_id_to_parquet = BuildFieldIdToParquetColumn(*file_meta, root_schema);

	unordered_map<idx_t, BaseStatistics> column_stats;
	for (idx_t col_idx = 0; col_idx < columns.size(); col_idx++) {
		auto &column = *columns[col_idx];
		if (!IsSupportedType(column.type)) {
			continue;
		}
		auto field_it = field_id_to_parquet.find(column.id);
		if (field_it == field_id_to_parquet.end()) {
			//! This Iceberg column has no matching field id in this Parquet file (added column / name-mapped).
			continue;
		}
		auto parquet_col_idx = field_it->second;
		auto &parquet_column = root_schema.children[parquet_col_idx];

		//! Merge the per-row-group statistics, requiring *every* row group to carry exact min/max.
		unique_ptr<BaseStatistics> merged;
		bool all_exact = true;
		for (idx_t rg = 0; rg < file_meta->row_groups.size(); rg++) {
			if (!ColumnChunkMinMaxIsExact(*file_meta, rg, parquet_col_idx)) {
				all_exact = false;
				break;
			}
			auto stats = parquet_column.Stats(*file_meta, reader->parquet_options, rg, file_meta->row_groups[rg].columns);
			if (!stats) {
				all_exact = false;
				break;
			}
			if (!merged) {
				merged = std::move(stats);
			} else {
				merged->Merge(*stats);
			}
		}
		if (!all_exact || !merged) {
			continue;
		}

		StorageIndex storage_index;
		if (table) {
			storage_index = table->GetStorageIndex(ColumnIndex(col_idx));
		} else {
			storage_index = StorageIndex::FromColumnIndex(ColumnIndex(col_idx));
		}
		column_stats.emplace(storage_index.GetPrimaryIndex(), std::move(*merged));
	}

	partition_stats.partition_row_group = make_shared_ptr<IcebergParquetPartitionRowGroup>(std::move(column_stats));
	result.push_back(std::move(partition_stats));
}

unique_ptr<BaseStatistics> IcebergParquetPartitionRowGroup::GetColumnStatistics(const StorageIndex &storage_index) {
	auto it = column_stats.find(storage_index.GetPrimaryIndex());
	if (it == column_stats.end()) {
		return nullptr;
	}
	return it->second.ToUnique();
}

bool IcebergParquetPartitionRowGroup::MinMaxIsExact(const BaseStatistics &stats, const StorageIndex &storage_index) {
	//! We only store statistics for columns whose Parquet footer reported exact min/max for every row group.
	return column_stats.find(storage_index.GetPrimaryIndex()) != column_stats.end();
}

} // namespace duckdb
