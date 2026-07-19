#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "h5_read_shared.hpp"
#include "h5_raii.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#if __has_include("duckdb/common/vector/array_vector.hpp")
#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#else
#include "duckdb/common/types/vector.hpp"
#endif
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include <utility>
#include <vector>
#include <string>
#include <limits>
#include <variant>
#include <optional>
#include <algorithm>
#include <cmath>
#include <unordered_map>
#include <unordered_set>
#include <map>

namespace duckdb {

template <class T>
static T &GetStructChild(T &child) {
	return child;
}

template <class T>
static T &GetStructChild(unique_ptr<T> &child) {
	return *child;
}

static constexpr idx_t H5_READ_WIDE_ROW_THRESHOLD_BYTES = 64 * 1024;
// Bounds the combined storage of a column's one or two cache windows.
static constexpr idx_t H5_READ_CACHE_LIMIT_BYTES = 128 * 1024 * 1024;

// =============================================================================
// Type-safe index wrappers for projection pushdown
// =============================================================================
// These prevent mixing global schema indices and local scan indices.
// GlobalColumnIdx indexes the shared output schema (and therefore each file-local
// bind view's columns vector); LocalColumnIdx indexes dense scan arrays.

struct LocalColumnIdx {
	idx_t index; // Index into column_states [0, 1, 2, ...]
	explicit LocalColumnIdx(idx_t i) : index(i) {
	}
	operator idx_t() const {
		return index;
	}
};

struct GlobalColumnIdx {
	idx_t index; // Index into the shared output schema / file-local bind view columns
	explicit GlobalColumnIdx(idx_t i) : index(i) {
	}
	operator idx_t() const {
		return index;
	}
};

// ==================== h5_read Implementation ====================

// Regular column specification
struct RegularColumnSpec {
	std::string path;
	std::string column_name;
	LogicalType column_type;
	bool is_string;
	std::optional<H5TypeHandle> string_h5_type; // Present only for string datasets
	int ndims;
	std::vector<hsize_t> dims;
	// Per-column DuckDB output footprint for one scan row.
	idx_t output_bytes_per_row; // Zero for strings or zero-sized values
	idx_t elements_per_row;     // Output elements in the same scan-row value
};

// Scalar dataset specification (rank-0 value or null dataspace)
struct ScalarColumnSpec {
	std::string path;
	std::string column_name;
	LogicalType column_type;
	bool is_null_dataspace = false;
	std::optional<H5TypeHandle> string_h5_type; // Present only for non-null string datasets
};

enum class RunEncodingKind : uint8_t { START, END };

static const char *RunEncodingName(RunEncodingKind encoding) {
	return encoding == RunEncodingKind::START ? "RSE" : "REE";
}

static const char *RunEncodingTag(RunEncodingKind encoding) {
	return encoding == RunEncodingKind::START ? "rse" : "ree";
}

static const char *RunBoundaryName(RunEncodingKind encoding) {
	return encoding == RunEncodingKind::START ? "run_starts" : "run_ends";
}

// Run-encoded column specification
struct RunEncodedColumnSpec {
	RunEncodingKind encoding = RunEncodingKind::START;
	std::string boundaries_path;
	std::string values_path;
	std::string column_name;
	LogicalType column_type;
	std::optional<H5TypeHandle> values_string_h5_type; // Present only for string values datasets
};

static string FormatRunEncodedDatasetPairError(const string &message, const string &filename,
                                               const RunEncodedColumnSpec &spec) {
	auto base_message = message + " for " + RunBoundaryName(spec.encoding) + " dataset: " + spec.boundaries_path +
	                    ", values dataset: " + spec.values_path + " in file: " + filename;
	return AppendRemoteError(base_message, filename);
}

// Virtual index column specification
struct IndexColumnSpec {
	std::string column_name;
	LogicalType column_type;
};

// A column can be regular, scalar, run-encoded, or virtual index
using ColumnSpec = std::variant<RegularColumnSpec, ScalarColumnSpec, RunEncodedColumnSpec, IndexColumnSpec>;

// Logical row window stored by the extension cache.
struct CacheWindow {
	using CacheStorage =
	    std::variant<std::monostate, // Uninitialized
	                 std::vector<int8_t>, std::vector<int16_t>, std::vector<int32_t>, std::vector<int64_t>,
	                 std::vector<uint8_t>, std::vector<uint16_t>, std::vector<uint32_t>, std::vector<uint64_t>,
	                 std::vector<float>, std::vector<double>>;
	CacheStorage cache;

	// end_row is one past the logical cache window start plus window_rows.
	// This makes the cache window start recoverable as (end_row - window_rows).
	// Zero marks an uninitialized window as reusable.
	std::atomic<idx_t> end_row {0};
};

struct RegularColumnCache {
	static constexpr idx_t MAX_WINDOWS = 2;

	idx_t window_rows = 0;
	CacheWindow windows[MAX_WINDOWS]; // Atomic members prevent std::vector usage
};

// Regular column runtime state
struct RegularColumnState {
	H5DatasetHandle dataset;      // RAII wrapper - automatic cleanup
	H5DataspaceHandle file_space; // Cached dataspace handle (reused across reads)

	std::unique_ptr<RegularColumnCache> cache;
};

// Scalar column runtime state (cached value)
struct ScalarColumnState {
	using ScalarValue = std::variant<std::monostate, int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t,
	                                 uint64_t, float, double, string>;
	ScalarValue value;
};

// Run-encoded column runtime state
struct RunEncodedColumnState {
	std::vector<idx_t> run_starts;
	idx_t non_null_end = 0;

	// Typed storage for values (eliminates Value object overhead)
	using RunEncodedValueStorage =
	    std::variant<std::vector<int8_t>, std::vector<int16_t>, std::vector<int32_t>, std::vector<int64_t>,
	                 std::vector<uint8_t>, std::vector<uint16_t>, std::vector<uint32_t>, std::vector<uint64_t>,
	                 std::vector<float>, std::vector<double>, std::vector<string>>;
	RunEncodedValueStorage values;

	// Note: No mutable state needed - ScanRunEncodedColumn is now stateless (thread-safe)
};

struct IndexColumnState {};

// Runtime state for a column (regular, scalar, run-encoded, or index)
using ColumnState = std::variant<RegularColumnState, ScalarColumnState, RunEncodedColumnState, IndexColumnState>;

// Row range for filtering (defined here for use in bind data and global state)
struct RowRange {
	idx_t start_row;
	idx_t end_row;
};

// Filter claimed during pushdown complex filter callback
struct ClaimedFilter {
	idx_t column_index;        // Which column (index into the shared output schema)
	ExpressionType comparison; // Comparison type (>, <, =, >=, <=)
	Value constant;            // The constant value to compare against
	LogicalType comparison_type;
};

struct PushdownColumnRef {
	const BoundColumnRefExpression *column_ref = nullptr;
	LogicalType comparison_type;
};

// Single-file bind data for the inner h5_read implementation.
struct H5ReadSingleFileBindData {
	std::string filename;
	vector<ColumnSpec> columns; // Unified column specifications
	hsize_t num_rows;           // Row count from regular datasets
	bool swmr = false;
};

struct H5ReadSingleFileBindView {
	const string &filename;
	const vector<ColumnSpec> &columns;
	hsize_t num_rows;
	const vector<ClaimedFilter> &claimed_filters;
	bool swmr = false;
};

// Data for h5_read table function.
struct H5ReadBindData : public TableFunctionData {
	vector<H5ReadSingleFileBindData> file_bind_data;
	hsize_t total_num_rows = 0;            // Total row count across all matched files
	vector<ClaimedFilter> claimed_filters; // Filters we claimed during pushdown
	std::optional<idx_t> visible_filename_idx;

	bool SupportStatementCache() const override {
		return false;
	}
};

// File-local runtime state for the existing single-file h5_read scan path.
// This no longer participates directly in DuckDB's table-function API.
struct H5ReadGlobalState {
	H5FileHandle file;                 // RAII wrapper for file handle
	vector<ColumnState> column_states; // DENSE array: indexed by LOCAL position [0, 1, 2, ...]

	// Projection pushdown support
	vector<column_t> columns_to_scan;            // Global column indices into the shared output schema
	vector<idx_t> output_column_positions;       // Output chunk positions for scanned columns
	unordered_map<idx_t, idx_t> global_to_local; // Maps global column idx -> local column_states idx
	vector<LocalColumnIdx> cache_refresh_order;  // Refreshable regular columns in file-friendly order

	// Position tracking
	// position is the next globally unclaimed scan range start.
	idx_t position = 0;                   // Protected by range_selection_mutex
	std::atomic<idx_t> position_done {0}; // All rows in [0, position_done) have been returned or filtered out

	// Row range filtering (for predicate pushdown on run-encoded or index columns)
	vector<RowRange> valid_row_ranges; // Sorted, non-overlapping ranges to scan
	idx_t scan_batch_size = STANDARD_VECTOR_SIZE;

	// Mutex for thread-safe range selection (enables parallel scanning)
	std::mutex range_selection_mutex;

	// Track out-of-order scan completions for position_done advancement
	// Maps scan start position -> scan end position for completed scans
	// that couldn't be merged into position_done yet (due to gaps)
	std::map<idx_t, idx_t> completed_ranges;

	// Cache loading coordination: only one thread loads windows at a time
	// Other threads proceed with scanning cached data (enables parallel processing)
	std::atomic<bool> someone_is_fetching {false};

	// No destructor needed - RAII wrappers handle all cleanup automatically
};

struct H5ReadMultiFileGlobalState : public GlobalTableFunctionState {
	vector<column_t> data_column_ids;
	vector<idx_t> data_output_column_positions;
	vector<idx_t> filename_output_positions;
	vector<idx_t> empty_output_positions;
	shared_ptr<H5ReadGlobalState> current_file;
	idx_t current_file_idx = 0;
	std::mutex current_file_lock;

	idx_t MaxThreads() const override {
		return GlobalTableFunctionState::MAX_THREADS;
	}
};

struct H5ReadMultiFileLocalState : public LocalTableFunctionState {
	shared_ptr<H5ReadGlobalState> file;
	idx_t file_idx = 0;
};

// =============================================================================
// Helper functions for safe column index mapping
// =============================================================================

// Map global column index to local column_states index
// Returns LocalColumnIdx if found, throws if not (indicates DuckDB bug or our bug)
static LocalColumnIdx GlobalToLocal(const H5ReadGlobalState &gstate, GlobalColumnIdx global_idx) {
	auto it = gstate.global_to_local.find(global_idx.index);
	if (it == gstate.global_to_local.end()) {
		throw InternalException("Column index %llu not in projection - this is a bug", global_idx.index);
	}
	return LocalColumnIdx(it->second);
}

// Get global column index from columns_to_scan by local position
static GlobalColumnIdx GetGlobalIdx(const H5ReadGlobalState &gstate, LocalColumnIdx local_idx) {
	if (local_idx.index >= gstate.columns_to_scan.size()) {
		throw InternalException("Local index %llu out of range (size=%llu)", local_idx.index,
		                        gstate.columns_to_scan.size());
	}
	return GlobalColumnIdx(gstate.columns_to_scan[local_idx.index]);
}

// Get number of columns being scanned (size of dense arrays)
static idx_t GetNumScannedColumns(const H5ReadGlobalState &gstate) {
	return gstate.columns_to_scan.size();
}

static bool H5ReadIsFilenameColumn(const H5ReadBindData &bind_data, column_t column_id) {
	return column_id == MultiFileReader::COLUMN_IDENTIFIER_FILENAME ||
	       (bind_data.visible_filename_idx.has_value() && column_id == *bind_data.visible_filename_idx);
}

static bool H5ReadIsDataColumn(const H5ReadBindData &bind_data, column_t column_id) {
	D_ASSERT(!bind_data.file_bind_data.empty());
	return column_id < bind_data.file_bind_data[0].columns.size();
}

struct CacheRefreshOrderEntry {
	LocalColumnIdx local_idx;
	std::optional<haddr_t> address;
	idx_t original_order = 0;
};

// Helper: get base type from a possibly nested collection type
static LogicalType GetBaseType(LogicalType type) {
	while (type.id() == LogicalTypeId::ARRAY || type.id() == LogicalTypeId::LIST) {
		if (type.id() == LogicalTypeId::ARRAY) {
			type = ArrayType::GetChildType(type);
		} else {
			type = ListType::GetChildType(type);
		}
	}
	return type;
}

// Helper: get innermost vector for array columns
static Vector &GetInnermostArrayVector(Vector &vector, const LogicalType &type) {
	Vector *current_vector = &vector;
	LogicalType current_type = type;
	while (current_type.id() == LogicalTypeId::ARRAY) {
		current_vector = &ArrayVector::GetEntry(*current_vector);
		current_type = ArrayType::GetChildType(current_type);
	}
	return *current_vector;
}

static Vector &PrepareRegularResultVector(Vector &result_vector, const RegularColumnSpec &spec, idx_t row_count,
                                          const string &filename) {
	if (spec.column_type.id() != LogicalTypeId::LIST) {
		return GetInnermostArrayVector(result_vector, spec.column_type);
	}

	Vector *current_vector = &result_vector;
	idx_t parent_count = row_count;
	for (int dimension_idx = 1; dimension_idx < spec.ndims; dimension_idx++) {
		D_ASSERT(current_vector->GetType().id() == LogicalTypeId::LIST);
		auto dimension = static_cast<idx_t>(spec.dims[dimension_idx]);
		auto child_count = CheckedDatasetSizeProduct(parent_count, dimension, filename, spec.path);

		ListVector::Reserve(*current_vector, child_count);
		auto entries = ListVector::GetData(*current_vector);
		for (idx_t parent_idx = 0; parent_idx < parent_count; parent_idx++) {
			entries[parent_idx] = list_entry_t(parent_idx * dimension, dimension);
		}
		ListVector::SetListSize(*current_vector, child_count);

		current_vector = &ListVector::GetEntry(*current_vector);
		parent_count = child_count;
	}

	return *current_vector;
}

// Helper function to generate column name from dataset path
static string GetColumnName(const string &dataset_path) {
	// Extract just the dataset name (last component of path)
	std::string col_name = dataset_path;

	// Find the last slash
	size_t last_slash = col_name.find_last_of('/');
	if (last_slash != std::string::npos) {
		col_name = col_name.substr(last_slash + 1);
	}

	// If empty (shouldn't happen), use default
	if (col_name.empty()) {
		col_name = "data";
	}

	return col_name;
}

static bool H5ReadOutputHasColumnName(const vector<string> &names, const string &column_name) {
	return std::any_of(names.begin(), names.end(),
	                   [&](const string &name) { return StringUtil::CIEquals(name, column_name); });
}

static void BuildH5ReadProjectionLayout(const H5ReadBindData &bind_data, const vector<column_t> &column_ids,
                                        vector<column_t> &data_column_ids, vector<idx_t> &data_output_positions,
                                        vector<idx_t> &filename_output_positions,
                                        vector<idx_t> &empty_output_positions) {
	data_column_ids.clear();
	data_output_positions.clear();
	filename_output_positions.clear();
	empty_output_positions.clear();

	D_ASSERT(!bind_data.file_bind_data.empty());
	const auto canonical_column_count = bind_data.file_bind_data[0].columns.size();
	if (column_ids.empty()) {
		data_column_ids.reserve(canonical_column_count);
		data_output_positions.reserve(canonical_column_count);
		for (idx_t i = 0; i < canonical_column_count; i++) {
			data_column_ids.push_back(i);
			data_output_positions.push_back(i);
		}
		if (bind_data.visible_filename_idx.has_value()) {
			filename_output_positions.push_back(*bind_data.visible_filename_idx);
		}
		return;
	}

	data_column_ids.reserve(column_ids.size());
	data_output_positions.reserve(column_ids.size());
	filename_output_positions.reserve(column_ids.size());
	empty_output_positions.reserve(column_ids.size());
	for (idx_t output_idx = 0; output_idx < column_ids.size(); output_idx++) {
		auto column_id = column_ids[output_idx];
		if (H5ReadIsDataColumn(bind_data, column_id)) {
			data_column_ids.push_back(column_id);
			data_output_positions.push_back(output_idx);
		} else if (H5ReadIsFilenameColumn(bind_data, column_id)) {
			filename_output_positions.push_back(output_idx);
		} else if (column_id == COLUMN_IDENTIFIER_EMPTY) {
			empty_output_positions.push_back(output_idx);
		}
	}
}

// Helper function to build collection types for multi-dimensional datasets.
// Wide rows use LIST so DuckDB does not eagerly allocate STANDARD_VECTOR_SIZE full rows.
static LogicalType BuildCollectionType(LogicalType base_type, const std::vector<hsize_t> &dims, int ndims,
                                       bool uses_nested_lists, const string &filename, const string &dataset_path) {
	if (ndims <= 1) {
		return base_type;
	}

	if (ndims > 4) {
		throw IOException(FormatDatasetError("Datasets with more than 4 dimensions are not currently supported",
		                                     filename, dataset_path));
	}

	// Build nested collection types from innermost to outermost.
	LogicalType result = base_type;
	for (int i = ndims - 1; i >= 1; i--) {
		if (uses_nested_lists) {
			result = LogicalType::LIST(result);
		} else {
			result = LogicalType::ARRAY(result, dims[i]);
		}
	}
	return result;
}

static idx_t H5ReadNumericOutputElementSize(const LogicalType &base_type) {
	return DispatchOnNumericType(base_type, [&](auto type_tag) -> idx_t {
		using T = typename decltype(type_tag)::type;
		return sizeof(T);
	});
}

// Helper: select hyperslab and create matching memory dataspace
static H5DataspaceHandle CreateMemspaceAndSelect(hid_t file_space_id, const RegularColumnSpec &spec, idx_t position,
                                                 idx_t to_read) {
	H5DataspaceHandle mem_space;
	if (spec.ndims == 1) {
		hsize_t start[1] = {position};
		hsize_t count[1] = {to_read};
		H5Sselect_hyperslab(file_space_id, H5S_SELECT_SET, start, nullptr, count, nullptr);

		hsize_t mem_dims[1] = {to_read};
		mem_space = H5DataspaceHandle(1, mem_dims);
	} else {
		std::vector<hsize_t> start(spec.ndims, 0);
		std::vector<hsize_t> count(spec.ndims);
		start[0] = position;
		count[0] = to_read;
		for (int i = 1; i < spec.ndims; i++) {
			count[i] = spec.dims[i];
		}
		H5Sselect_hyperslab(file_space_id, H5S_SELECT_SET, start.data(), nullptr, count.data(), nullptr);

		std::vector<hsize_t> mem_dims(spec.ndims);
		mem_dims[0] = to_read;
		for (int i = 1; i < spec.ndims; i++) {
			mem_dims[i] = spec.dims[i];
		}
		mem_space = H5DataspaceHandle(spec.ndims, mem_dims.data());
	}
	return mem_space;
}

static idx_t ComputeCacheWindowRows(const RegularColumnSpec &spec, hid_t dataset_id, idx_t target_batch_size_bytes,
                                    idx_t total_rows) {
	idx_t chunk_rows = 0;
	hid_t dcpl = H5Dget_create_plist(dataset_id);
	if (dcpl >= 0) {
		H5D_layout_t layout = H5Pget_layout(dcpl);
		if (layout == H5D_CHUNKED) {
			std::vector<hsize_t> chunk_dims(spec.ndims);
			if (H5Pget_chunk(dcpl, spec.ndims, chunk_dims.data()) >= 0 && chunk_dims[0] > 0) {
				chunk_rows = chunk_dims[0];
			}
		}
		H5Pclose(dcpl);
	}

	idx_t window_rows;
	if (chunk_rows > 0) {
		auto row_bytes = MaxValue<idx_t>(spec.output_bytes_per_row, 1);
		idx_t target_rows = MaxValue<idx_t>(target_batch_size_bytes / row_bytes, 1);
		target_rows = MaxValue<idx_t>(target_rows, chunk_rows);
		idx_t remainder = target_rows % chunk_rows;
		if (remainder != 0) {
			target_rows += (chunk_rows - remainder);
		}
		window_rows = target_rows;
		D_ASSERT(window_rows % chunk_rows == 0);
	} else {
		window_rows = MaxValue<idx_t>(target_batch_size_bytes / MaxValue<idx_t>(spec.output_bytes_per_row, 1), 1);
	}
	if (total_rows == 0) {
		return 0;
	}
	return MinValue<idx_t>(window_rows, total_rows);
}

static idx_t ComputeCacheWindowCount(idx_t window_rows, idx_t total_rows) {
	return window_rows < total_rows ? RegularColumnCache::MAX_WINDOWS : 1;
}

static bool H5ReadShouldCreateCache(const RegularColumnSpec &spec, idx_t window_rows, idx_t total_rows,
                                    idx_t scan_batch_size) {
	D_ASSERT(spec.output_bytes_per_row > 0);
	auto window_count = ComputeCacheWindowCount(window_rows, total_rows);
	auto max_window_rows = H5_READ_CACHE_LIMIT_BYTES / window_count / spec.output_bytes_per_row;
	return window_rows > scan_batch_size && window_rows <= max_window_rows;
}

static std::optional<haddr_t> TryGetDatasetReadOrderAddress(const RegularColumnSpec &spec, hid_t dataset_id) {
	hid_t dcpl = H5Dget_create_plist(dataset_id);
	if (dcpl < 0) {
		return std::nullopt;
	}

	std::optional<haddr_t> result;
	auto layout = H5Pget_layout(dcpl);
	if (layout == H5D_CONTIGUOUS) {
		auto address = H5Dget_offset(dataset_id);
		if (address != HADDR_UNDEF) {
			result = address;
		}
	} else if (layout == H5D_CHUNKED) {
		// Use the first logical chunk as a cheap proxy for physical dataset order.
		// This is only a heuristic, but it is enough to avoid obvious "wrong way around"
		// refresh order for interleaved multi-dataset scans.
		std::vector<hsize_t> chunk_origin(spec.ndims, 0);
		unsigned filter_mask = 0;
		haddr_t chunk_address = HADDR_UNDEF;
		hsize_t physical_chunk_bytes = 0;
		if (H5Dget_chunk_info_by_coord(dataset_id, chunk_origin.data(), &filter_mask, &chunk_address,
		                               &physical_chunk_bytes) >= 0 &&
		    chunk_address != HADDR_UNDEF && physical_chunk_bytes > 0) {
			result = chunk_address;
		}
	}

	H5Pclose(dcpl);
	return result;
}

// Predicate Pushdown Helpers (for run-encoded columns)
//===--------------------------------------------------------------------===//

static std::vector<RowRange>::const_iterator FindRangeForPosition(const std::vector<RowRange> &ranges, idx_t position) {
	return std::lower_bound(ranges.begin(), ranges.end(), position,
	                        [](const RowRange &range, idx_t pos) { return range.end_row <= pos; });
}

static idx_t AdjustPositionDoneForRanges(const std::vector<RowRange> &ranges, idx_t position_done) {
	auto it = FindRangeForPosition(ranges, position_done);
	if (it == ranges.end()) {
		return position_done;
	}
	if (position_done < it->start_row) {
		return it->start_row;
	}
	return position_done;
}

enum class H5ReadFilterEvalResult : uint8_t { FALSE, TRUE, UNKNOWN };

static H5ReadFilterEvalResult EvaluateValueComparison(const Value &value, ExpressionType comparison,
                                                      const Value &filter_val, const LogicalType &comparison_type);
static vector<RowRange> IntersectRowRanges(const vector<RowRange> &a, const vector<RowRange> &b);

static bool EvaluateIndexComparison(idx_t index, const ClaimedFilter &filter, ExpressionType comparison) {
	auto result = EvaluateValueComparison(Value::BIGINT(static_cast<int64_t>(index)), comparison, filter.constant,
	                                      filter.comparison_type);
	if (result == H5ReadFilterEvalResult::UNKNOWN) {
		throw InternalException("h5_read index pushdown admitted a comparison that cannot be evaluated");
	}
	return result == H5ReadFilterEvalResult::TRUE;
}

static vector<RowRange> BuildIndexRanges(const vector<ClaimedFilter> &filters, idx_t num_rows) {
	auto build_single_filter_ranges = [&](const ClaimedFilter &filter) -> vector<RowRange> {
		auto find_first_true = [&](ExpressionType comparison) -> idx_t {
			idx_t lo = 0;
			idx_t hi = num_rows;
			while (lo < hi) {
				idx_t mid = lo + (hi - lo) / 2;
				if (EvaluateIndexComparison(mid, filter, comparison)) {
					hi = mid;
				} else {
					lo = mid + 1;
				}
			}
			return lo;
		};

		auto find_first_false = [&](ExpressionType comparison) -> idx_t {
			idx_t lo = 0;
			idx_t hi = num_rows;
			while (lo < hi) {
				idx_t mid = lo + (hi - lo) / 2;
				if (EvaluateIndexComparison(mid, filter, comparison)) {
					lo = mid + 1;
				} else {
					hi = mid;
				}
			}
			return lo;
		};

		switch (filter.comparison) {
		case ExpressionType::COMPARE_EQUAL: {
			auto start = find_first_true(ExpressionType::COMPARE_GREATERTHANOREQUALTO);
			auto end = find_first_false(ExpressionType::COMPARE_LESSTHANOREQUALTO);
			if (start >= end) {
				return {};
			}
			return {{start, end}};
		}
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
			auto start = find_first_true(filter.comparison);
			if (start >= num_rows) {
				return {};
			}
			return {{start, num_rows}};
		}
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
			auto end = find_first_false(filter.comparison);
			if (end == 0) {
				return {};
			}
			return {{0, end}};
		}
		default:
			return {};
		}
	};

	vector<RowRange> result = {{0, num_rows}};
	for (const auto &filter : filters) {
		result = IntersectRowRanges(result, build_single_filter_ranges(filter));
		if (result.empty()) {
			break;
		}
	}
	return result;
}

static H5ReadFilterEvalResult EvaluateValueComparison(const Value &value, ExpressionType comparison,
                                                      const Value &filter_val, const LogicalType &comparison_type) {
	Value lhs = value;
	Value rhs = filter_val;
	if (comparison_type.IsValid()) {
		if (!lhs.DefaultTryCastAs(comparison_type, true) || !rhs.DefaultTryCastAs(comparison_type, true)) {
			return H5ReadFilterEvalResult::UNKNOWN;
		}
	}
	if (lhs.IsNull() || rhs.IsNull()) {
		return H5ReadFilterEvalResult::FALSE;
	}
	bool result;
	switch (comparison) {
	case ExpressionType::COMPARE_EQUAL:
		result = ValueOperations::Equals(lhs, rhs);
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		result = ValueOperations::GreaterThan(lhs, rhs);
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		result = ValueOperations::GreaterThanEquals(lhs, rhs);
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		result = ValueOperations::LessThan(lhs, rhs);
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		result = ValueOperations::LessThanEquals(lhs, rhs);
		break;
	default:
		return H5ReadFilterEvalResult::FALSE;
	}
	return result ? H5ReadFilterEvalResult::TRUE : H5ReadFilterEvalResult::FALSE;
}

static vector<RowRange> BuildRangesForRunEncodedColumn(const RunEncodedColumnSpec &encoded_spec,
                                                       const RunEncodedColumnState &encoded_state,
                                                       const vector<ClaimedFilter> &col_filters) {
	return DispatchOnDuckDBType(encoded_spec.column_type, [&](auto type_tag) -> vector<RowRange> {
		using T = typename decltype(type_tag)::type;
		auto &typed_values = std::get<std::vector<T>>(encoded_state.values);
		D_ASSERT(encoded_state.run_starts.size() == typed_values.size());

		// Loop through runs, building ranges where ALL filters are satisfied
		vector<RowRange> col_result;
		idx_t current_start = 0;
		bool in_range = false;

		// Leading NULL segment (if any) never satisfies comparison filters.
		if (!encoded_state.run_starts.empty()) {
			current_start = encoded_state.run_starts[0];
		}

		for (size_t i = 0; i < typed_values.size(); i++) {
			const T &value = typed_values[i];
			Value run_value = Value::CreateValue(value);
			idx_t run_start = encoded_state.run_starts[i];

			// Check if this run's value satisfies ALL filters
			bool satisfies_all = true;
			for (const auto &filter : col_filters) {
				auto result =
				    EvaluateValueComparison(run_value, filter.comparison, filter.constant, filter.comparison_type);
				if (result == H5ReadFilterEvalResult::FALSE) {
					satisfies_all = false;
					break;
				}
			}

			if (satisfies_all && !in_range) {
				// Start new range
				current_start = run_start;
				in_range = true;
			} else if (!satisfies_all && in_range) {
				// End current range
				if (current_start < run_start) {
					col_result.push_back({current_start, run_start});
				}
				in_range = false;
			}
		}

		// Close final range if still open. Rows at or after non_null_end are NULLs.
		if (in_range && current_start < encoded_state.non_null_end) {
			col_result.push_back({current_start, encoded_state.non_null_end});
		}

		return col_result;
	});
}

//===--------------------------------------------------------------------===//
// Run-Encoding Helpers
//===--------------------------------------------------------------------===//

static vector<idx_t> LoadRunBoundaries(const string &filename, const RunEncodedColumnSpec &spec, hid_t boundaries_ds,
                                       size_t num_runs, hsize_t num_rows, idx_t &non_null_end) {
	vector<idx_t> boundaries(num_runs);
	H5ErrorSuppressor suppress;
	herr_t status = H5Dread(boundaries_ds, H5T_NATIVE_UINT64, H5S_ALL, H5S_ALL, H5P_DEFAULT, boundaries.data());
	if (status < 0) {
		throw IOException(FormatDatasetError(string("Failed to read ") + RunBoundaryName(spec.encoding) + " from",
		                                     filename, spec.boundaries_path));
	}

	if (!std::is_sorted(boundaries.begin(), boundaries.end())) {
		throw IOException(FormatDatasetError(string(RunEncodingName(spec.encoding)) + " " +
		                                         RunBoundaryName(spec.encoding) + " must be non-decreasing",
		                                     filename, spec.boundaries_path));
	}

	const auto row_count = static_cast<idx_t>(num_rows);
	auto clamp_to_row_count = [row_count](idx_t boundary) {
		return std::min(boundary, row_count);
	};
	auto exclusive_end_from_inclusive = [row_count](idx_t run_end) {
		// Clamp before adding 1 to prevent hypothetical overflow.
		if (run_end >= row_count) {
			return row_count;
		}
		return run_end + 1;
	};

	if (spec.encoding == RunEncodingKind::START) {
		non_null_end = row_count;
		std::transform(boundaries.begin(), boundaries.end(), boundaries.begin(), clamp_to_row_count);
		return boundaries;
	}

	non_null_end = 0;
	if (num_runs == 0) {
		return boundaries;
	}

	vector<idx_t> run_starts(num_runs);
	run_starts[0] = 0;
	for (size_t i = 1; i < num_runs; i++) {
		run_starts[i] = exclusive_end_from_inclusive(boundaries[i - 1]);
	}
	non_null_end = exclusive_end_from_inclusive(boundaries.back());
	return run_starts;
}

static RunEncodedColumnState::RunEncodedValueStorage
LoadRunEncodedValues(const string &filename, const RunEncodedColumnSpec &spec, hid_t values_ds, size_t num_values) {
	return DispatchOnDuckDBType(spec.column_type, [&](auto type_tag) -> RunEncodedColumnState::RunEncodedValueStorage {
		using T = typename decltype(type_tag)::type;

		if constexpr (std::is_same_v<T, string>) {
			D_ASSERT(spec.values_string_h5_type.has_value());
			std::vector<string> string_values;
			string_values.reserve(num_values);
			ReadHDF5Strings(values_ds, *spec.values_string_h5_type, H5S_ALL, H5S_ALL, num_values, filename,
			                spec.values_path, [&](idx_t i, const std::string &str) { string_values.push_back(str); });
			return string_values;
		} else {
			std::vector<T> typed_values(num_values);
			H5ErrorSuppressor suppress;
			herr_t status =
			    H5Dread(values_ds, GetNativeH5Type<T>(), H5S_ALL, H5S_ALL, H5P_DEFAULT, typed_values.data());
			if (status < 0) {
				throw IOException(FormatDatasetError("Failed to read values from", filename, spec.values_path));
			}
			return typed_values;
		}
	});
}

static bool IsAliasStructType(const LogicalType &type) {
	if (type.id() != LogicalTypeId::STRUCT) {
		return false;
	}
	auto &children = StructType::GetChildTypes(type);
	if (children.size() != 3) {
		return false;
	}
	return children[0].second == LogicalType::VARCHAR;
}

static bool IsIndexStructType(const LogicalType &type) {
	if (type.id() != LogicalTypeId::STRUCT) {
		return false;
	}
	auto &children = StructType::GetChildTypes(type);
	if (children.size() != 1) {
		return false;
	}
	return children[0].second == LogicalType::VARCHAR;
}

static Value UnwrapAliasSpec(const Value &input, std::optional<std::string> &alias_name) {
	Value current = input;
	while (IsAliasStructType(current.type())) {
		auto &children = StructValue::GetChildren(current);
		if (children.size() != 3) {
			throw InvalidInputException("h5_alias() must return a struct with 3 fields");
		}
		if (children[0].GetValue<string>() != "__alias__") {
			break;
		}
		if (!alias_name) {
			alias_name = children[1].GetValue<string>();
		}
		current = children[2];
	}
	return current;
}

//===--------------------------------------------------------------------===//
// h5_read - Read datasets from HDF5 files
//===--------------------------------------------------------------------===//

static void PopulateH5ReadOutputSchema(const vector<ColumnSpec> &columns, vector<LogicalType> &return_types,
                                       vector<string> &names) {
	names.clear();
	return_types.clear();
	for (const auto &col : columns) {
		std::visit(
		    [&](auto &&spec) {
			    names.push_back(spec.column_name);
			    return_types.push_back(spec.column_type);
		    },
		    col);
	}
}

static H5ReadSingleFileBindData BindSingleH5ReadFile(ClientContext &context, const string &filename, bool swmr,
                                                     const vector<Value> &inputs) {
	H5ReadSingleFileBindData result;
	result.filename = filename;
	result.swmr = swmr;
	size_t num_columns = inputs.size() - 1;

	// Lock for all HDF5 operations (not thread-safe)
	std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);

	// Open file once (with error suppression) - RAII wrapper handles cleanup
	H5FileHandle file;
	{
		H5ErrorSuppressor suppress;
		file = H5FileHandle(&context, result.filename.c_str(), H5F_ACC_RDONLY, result.swmr);
	}

	if (!file.is_valid()) {
		throw IOException(FormatRemoteHDF5Error("Failed to open HDF5 file", result.filename));
	}

	// Track minimum rows across all non-scalar regular columns
	hsize_t min_rows = std::numeric_limits<hsize_t>::max();
	size_t num_regular_columns = 0;
	size_t non_scalar_regular_columns = 0;
	bool has_run_encoded_columns = false;

	// Process each column (regular dataset, run-encoded, or index)
	for (size_t i = 0; i < num_columns; i++) {
		const auto &input_val = inputs[i + 1];
		std::optional<std::string> alias_name;
		Value column_val = UnwrapAliasSpec(input_val, alias_name);

		// Check for virtual index or run-encoded column (STRUCT type)
		if (column_val.type().id() == LogicalTypeId::STRUCT) {
			auto &children = StructValue::GetChildren(column_val);

			if (IsIndexStructType(column_val.type())) {
				if (children[0].GetValue<string>() != "__index__") {
					throw InvalidInputException("Unknown struct argument for h5_read");
				}

				IndexColumnSpec index_spec;
				index_spec.column_name = alias_name ? *alias_name : "index";
				index_spec.column_type = LogicalType::BIGINT;

				result.columns.push_back(std::move(index_spec));
				continue;
			}

			// Run-encoded column - extract struct fields
			if (children.size() != 3) {
				throw InvalidInputException("Expected h5_rse() or h5_ree() to return a struct with 3 fields");
			}

			string encoding = children[0].GetValue<string>();
			string boundaries = children[1].GetValue<string>();
			string values = children[2].GetValue<string>();

			RunEncodingKind encoding_kind;
			if (encoding == "rse") {
				encoding_kind = RunEncodingKind::START;
			} else if (encoding == "ree") {
				encoding_kind = RunEncodingKind::END;
			} else {
				throw InvalidInputException("Unknown encoding: " + encoding);
			}

			RunEncodedColumnSpec encoded_spec;
			encoded_spec.encoding = encoding_kind;
			encoded_spec.boundaries_path = boundaries;
			encoded_spec.values_path = values;
			encoded_spec.column_name = alias_name ? *alias_name : GetColumnName(values);
			has_run_encoded_columns = true;

			// Open boundary dataset and get type
			auto [boundaries_ds, boundaries_type] = OpenDatasetAndGetType(file, result.filename, boundaries);
			if (H5Tget_class(boundaries_type) != H5T_INTEGER) {
				throw IOException(FormatDatasetError(string(RunEncodingName(encoding_kind)) + " " +
				                                         RunBoundaryName(encoding_kind) + " must be integer type",
				                                     result.filename, boundaries));
			}

			// Open values dataset and get type
			auto [values_ds, values_type] = OpenDatasetAndGetType(file, result.filename, values);
			// Determine DuckDB column type from values (before move)
			encoded_spec.column_type = H5TypeToDuckDBType(values_type);
			if (H5Tget_class(values_type) == H5T_STRING) {
				encoded_spec.values_string_h5_type = std::move(values_type);
			}

			result.columns.push_back(std::move(encoded_spec));

		} else {
			// Regular column (may be scalar)
			if (column_val.type().id() != LogicalTypeId::VARCHAR) {
				throw InvalidInputException("h5_read dataset path arguments must be VARCHAR, h5_rse(), h5_ree(), "
				                            "h5_index(), or h5_alias(...)");
			}
			RegularColumnSpec ds_info;
			ds_info.path = GetRequiredStringArgument(column_val, "h5_read", "dataset path");
			ds_info.column_name = alias_name ? *alias_name : GetColumnName(ds_info.path);
			num_regular_columns++;

			// Open dataset and get type
			auto [dataset, type] = OpenDatasetAndGetType(file, result.filename, ds_info.path);

			// Check if it's a string type
			ds_info.is_string = (H5Tget_class(type) == H5T_STRING);

			// Get dataspace to determine dimensions - RAII handles cleanup
			H5DataspaceHandle space(dataset);
			if (!space.is_valid()) {
				throw IOException(FormatDatasetError("Failed to get dataset dataspace", result.filename, ds_info.path));
			}

			auto space_class = H5Sget_simple_extent_type(space);
			if (space_class == H5S_NO_CLASS) {
				throw IOException(
				    FormatDatasetError("Failed to get dataset dataspace class", result.filename, ds_info.path));
			}

			if (space_class == H5S_SCALAR || space_class == H5S_NULL) {
				// Null dataspaces use scalar row and broadcast semantics with a NULL value.
				ScalarColumnSpec scalar_info;
				scalar_info.path = ds_info.path;
				scalar_info.column_name = ds_info.column_name;
				scalar_info.is_null_dataspace = space_class == H5S_NULL;

				scalar_info.column_type = H5TypeToDuckDBType(type);
				if (ds_info.is_string && !scalar_info.is_null_dataspace) {
					scalar_info.string_h5_type = std::move(type);
				}

				result.columns.push_back(std::move(scalar_info));
				continue;
			}
			if (space_class != H5S_SIMPLE) {
				throw IOException(
				    FormatDatasetError("Unsupported dataset dataspace class", result.filename, ds_info.path));
			}

			ds_info.ndims = H5Sget_simple_extent_ndims(space);
			if (ds_info.ndims < 0) {
				throw IOException(
				    FormatDatasetError("Failed to get dataset dimensions", result.filename, ds_info.path));
			}
			if (ds_info.is_string && ds_info.ndims > 1) {
				throw IOException(FormatDatasetError("String datasets with more than 1 dimension are not supported",
				                                     result.filename, ds_info.path));
			}

			ds_info.dims.resize(ds_info.ndims);
			H5Sget_simple_extent_dims(space, ds_info.dims.data(), nullptr);

			// Track minimum rows for non-scalar regular columns
			non_scalar_regular_columns++;
			if (ds_info.dims[0] < min_rows) {
				min_rows = ds_info.dims[0];
			}

			// Map HDF5 type to DuckDB type
			LogicalType base_type = H5TypeToDuckDBType(type);

			// Calculate DuckDB output bytes/elements for this column in one scan row.
			// This intentionally uses the DuckDB/native memory size, not the file size:
			// e.g. HDF5 float16 values are widened into DuckDB FLOAT values.
			ds_info.output_bytes_per_row = ds_info.is_string ? 0 : H5ReadNumericOutputElementSize(base_type);
			ds_info.elements_per_row = 1;
			for (int j = 1; j < ds_info.ndims; j++) {
				auto dimension = static_cast<idx_t>(ds_info.dims[j]);
				ds_info.output_bytes_per_row =
				    CheckedDatasetSizeProduct(ds_info.output_bytes_per_row, dimension, result.filename, ds_info.path);
				ds_info.elements_per_row =
				    CheckedDatasetSizeProduct(ds_info.elements_per_row, dimension, result.filename, ds_info.path);
			}
			auto uses_nested_lists =
			    ds_info.ndims > 1 && ds_info.output_bytes_per_row >= H5_READ_WIDE_ROW_THRESHOLD_BYTES;

			if (ds_info.is_string) {
				// Preserve file-local string metadata for runtime string decoding.
				ds_info.string_h5_type = std::move(type);
			}

			// Fixed ARRAY vectors eagerly allocate STANDARD_VECTOR_SIZE rows. Use nested
			// LIST vectors for wide rows so allocation follows the actual scan batch.
			ds_info.column_type = BuildCollectionType(base_type, ds_info.dims, ds_info.ndims, uses_nested_lists,
			                                          result.filename, ds_info.path);

			result.columns.push_back(std::move(ds_info));
		}
	}

	// Require at least one regular column (scalar or non-scalar)
	if (num_regular_columns == 0) {
		throw IOException("h5_read requires at least one regular column");
	}

	// Run-encoded columns require a non-scalar regular column for row count.
	if (has_run_encoded_columns && non_scalar_regular_columns == 0) {
		throw IOException(
		    "h5_read requires at least one non-scalar regular column when using run-encoded columns to determine row "
		    "count");
	}

	// Set the row count from non-scalar regular columns
	if (non_scalar_regular_columns > 0) {
		result.num_rows = min_rows;
	} else {
		// Only scalar datasets - return a single row
		result.num_rows = 1;
	}

	return result;
}

static bool H5ReadSchemasMatch(const H5ReadSingleFileBindData &expected, const H5ReadSingleFileBindData &actual) {
	if (expected.columns.size() != actual.columns.size()) {
		return false;
	}
	for (idx_t i = 0; i < expected.columns.size(); i++) {
		if (expected.columns[i].index() != actual.columns[i].index()) {
			return false;
		}
		// Multi-file h5_read requires every matched file to expose the same output
		// contract: the same column variant, output name, and DuckDB type at each
		// position. File-local runtime details are intentionally excluded here
		// because they are reopened and rebuilt per file during scan initialization.
		bool matches = false;
		std::visit(
		    [&](auto &&expected_spec, auto &&actual_spec) {
			    using ExpectedT = std::decay_t<decltype(expected_spec)>;
			    using ActualT = std::decay_t<decltype(actual_spec)>;
			    if constexpr (std::is_same_v<ExpectedT, ActualT>) {
				    matches = expected_spec.column_name == actual_spec.column_name &&
				              expected_spec.column_type == actual_spec.column_type;
				    if constexpr (std::is_same_v<ExpectedT, RegularColumnSpec>) {
					    // LIST types do not encode their extents. Preserve the previous
					    // multi-file requirement that inner dataset shapes match.
					    if (matches && expected_spec.column_type.id() == LogicalTypeId::LIST) {
						    matches = expected_spec.ndims == actual_spec.ndims &&
						              std::equal(expected_spec.dims.begin() + 1, expected_spec.dims.end(),
						                         actual_spec.dims.begin() + 1, actual_spec.dims.end());
					    }
				    }
				    if constexpr (std::is_same_v<ExpectedT, RunEncodedColumnSpec>) {
					    matches = matches && expected_spec.encoding == actual_spec.encoding;
				    }
			    }
		    },
		    expected.columns[i], actual.columns[i]);
		if (!matches) {
			return false;
		}
	}
	return true;
}

static const vector<ColumnSpec> &GetCanonicalColumns(const H5ReadBindData &bind_data) {
	D_ASSERT(!bind_data.file_bind_data.empty());
	return bind_data.file_bind_data[0].columns;
}

static idx_t EstimateScanBatchSize(const vector<ColumnSpec> &columns, const vector<column_t> &data_column_ids,
                                   idx_t target_batch_size_bytes) {
	D_ASSERT(target_batch_size_bytes > 0);
	// This estimates projected row width from regular non-string columns. Strings,
	// scalars, run-encoded, index, filename, and empty columns are not included.
	idx_t estimated_output_bytes_per_row = 0;
	for (auto column_id : data_column_ids) {
		if (auto regular_spec = std::get_if<RegularColumnSpec>(&columns[column_id]);
		    regular_spec && !regular_spec->is_string) {
			if (regular_spec->output_bytes_per_row >= target_batch_size_bytes - estimated_output_bytes_per_row) {
				return 1;
			}
			estimated_output_bytes_per_row += regular_spec->output_bytes_per_row;
		}
	}
	if (estimated_output_bytes_per_row == 0) {
		return STANDARD_VECTOR_SIZE;
	}
	return MinValue<idx_t>(target_batch_size_bytes / estimated_output_bytes_per_row, STANDARD_VECTOR_SIZE);
}

static H5ReadSingleFileBindView GetSingleFileBindView(const H5ReadBindData &bind_data, idx_t file_idx) {
	D_ASSERT(file_idx < bind_data.file_bind_data.size());
	auto &file_bind_data = bind_data.file_bind_data[file_idx];
	return {file_bind_data.filename, file_bind_data.columns, file_bind_data.num_rows, bind_data.claimed_filters,
	        file_bind_data.swmr};
}

// Bind function - expands glob patterns, validates schema, and records per-file row counts.
static unique_ptr<FunctionData> H5ReadBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	ThrowIfInterrupted(context);
	auto swmr = ResolveSwmrOption(context, input.named_parameters);
	auto filename_option = ResolveFilenameColumnOption(input.named_parameters);
	auto expanded = H5ExpandFilePatterns(context, input.inputs[0], "h5_read");
	D_ASSERT(!expanded.filenames.empty());

	auto first_file_bind = BindSingleH5ReadFile(context, expanded.filenames[0], swmr, input.inputs);
	PopulateH5ReadOutputSchema(first_file_bind.columns, return_types, names);
	if (filename_option.include) {
		if (H5ReadOutputHasColumnName(names, filename_option.column_name)) {
			throw BinderException("Option filename adds column \"%s\", but that column name is already present in "
			                      "h5_read output",
			                      filename_option.column_name);
		}
		names.push_back(filename_option.column_name);
		return_types.push_back(LogicalType::VARCHAR);
	}

	auto result = make_uniq<H5ReadBindData>();
	if (filename_option.include) {
		result->visible_filename_idx = names.size() - 1;
	}
	result->file_bind_data.reserve(expanded.filenames.size());
	result->total_num_rows = first_file_bind.num_rows;
	result->file_bind_data.push_back(std::move(first_file_bind));

	for (idx_t file_idx = 1; file_idx < expanded.filenames.size(); file_idx++) {
		auto file_bind = BindSingleH5ReadFile(context, expanded.filenames[file_idx], swmr, input.inputs);
		if (!H5ReadSchemasMatch(result->file_bind_data[0], file_bind)) {
			throw BinderException("h5_read matched file '%s' with an incompatible schema",
			                      expanded.filenames[file_idx]);
		}
		result->total_num_rows += file_bind.num_rows;
		result->file_bind_data.push_back(std::move(file_bind));
	}

	return result;
}

// Helper: Intersect two sorted lists of row ranges
static vector<RowRange> IntersectRowRanges(const vector<RowRange> &a, const vector<RowRange> &b) {
	vector<RowRange> result;

	size_t i = 0, j = 0;
	while (i < a.size() && j < b.size()) {
		idx_t start = std::max(a[i].start_row, b[j].start_row);
		idx_t end = std::min(a[i].end_row, b[j].end_row);

		// If ranges overlap, add the intersection
		if (start < end) {
			result.push_back({start, end});
		}

		// Advance the range that ends earlier
		if (a[i].end_row < b[j].end_row) {
			i++;
		} else {
			j++;
		}
	}

	return result;
}

static vector<RowRange> BuildRangesForColumn(GlobalColumnIdx global_idx, const vector<ClaimedFilter> &col_filters,
                                             const H5ReadSingleFileBindView &bind_data,
                                             const H5ReadGlobalState &gstate) {
	if (std::holds_alternative<RunEncodedColumnSpec>(bind_data.columns[global_idx])) {
		LocalColumnIdx local_idx = GlobalToLocal(gstate, global_idx);
		auto &encoded_spec = std::get<RunEncodedColumnSpec>(bind_data.columns[global_idx]);
		auto &encoded_state = std::get<RunEncodedColumnState>(gstate.column_states[local_idx]);
		return BuildRangesForRunEncodedColumn(encoded_spec, encoded_state, col_filters);
	}
	if (std::holds_alternative<IndexColumnSpec>(bind_data.columns[global_idx])) {
		return BuildIndexRanges(col_filters, bind_data.num_rows);
	}
	return {};
}

// Initialize the inner single-file scan state for one file.
static unique_ptr<H5ReadGlobalState> InitSingleH5ReadState(ClientContext &context,
                                                           const H5ReadSingleFileBindView &bind_data,
                                                           const vector<column_t> &data_column_ids,
                                                           const vector<idx_t> &data_output_positions) {
	ThrowIfInterrupted(context);
	auto result = make_uniq<H5ReadGlobalState>();
	auto target_batch_size_bytes = ResolveBatchSizeOption(context);

	result->columns_to_scan = data_column_ids;
	result->output_column_positions = data_output_positions;
	result->scan_batch_size = EstimateScanBatchSize(bind_data.columns, data_column_ids, target_batch_size_bytes);

	// Build global-to-local index mapping for projection pushdown
	// This allows O(1) lookup: global_column_idx -> local_column_states_idx
	for (idx_t local_idx = 0; local_idx < result->columns_to_scan.size(); local_idx++) {
		idx_t global_idx = result->columns_to_scan[local_idx];
		result->global_to_local[global_idx] = local_idx;
	}

	// Lock for all HDF5 operations (not thread-safe)
	std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);

	// Open file (with error suppression) - RAII wrapper handles cleanup
	{
		H5ErrorSuppressor suppress;
		result->file = H5FileHandle(&context, bind_data.filename.c_str(), H5F_ACC_RDONLY, bind_data.swmr);
	}

	if (!result->file.is_valid()) {
		throw IOException(FormatRemoteHDF5Error("Failed to open HDF5 file", bind_data.filename));
	}

	// Allocate DENSE column_states array - only for scanned columns
	// Indexed by LOCAL position [0, 1, 2, ...], not global column indices
	result->column_states.reserve(GetNumScannedColumns(*result));
	std::vector<CacheRefreshOrderEntry> cache_refresh_entries;

	// Process columns in scan order (builds dense array)
	for (idx_t i = 0; i < GetNumScannedColumns(*result); i++) {
		LocalColumnIdx local_idx(i);
		GlobalColumnIdx global_idx = GetGlobalIdx(*result, local_idx);
		const auto &col = bind_data.columns[global_idx];
		std::visit(
		    [&](auto &&spec) {
			    using T = std::decay_t<decltype(spec)>;

			    if constexpr (std::is_same_v<T, RegularColumnSpec>) {
				    // Regular column - open dataset (with error suppression)
				    H5DatasetHandle dataset;
				    {
					    H5ErrorSuppressor suppress;
					    dataset = H5DatasetHandle(result->file, spec.path.c_str());
				    }

				    if (!dataset.is_valid()) {
					    throw IOException(FormatDatasetError("Failed to open dataset", bind_data.filename, spec.path));
				    }

				    // Cache the file dataspace (reused across all reads)
				    H5DataspaceHandle file_space(dataset);
				    if (!file_space.is_valid()) {
					    throw IOException(
					        FormatDatasetError("Failed to get dataspace for dataset", bind_data.filename, spec.path));
				    }

				    RegularColumnState state;
				    state.dataset = std::move(dataset);
				    state.file_space = std::move(file_space);

				    // Create read-ahead cache windows for non-empty cacheable columns when one
				    // window can serve multiple output batches.
				    if (!spec.is_string && spec.output_bytes_per_row > 0) {
					    auto window_rows = ComputeCacheWindowRows(spec, state.dataset.get(), target_batch_size_bytes,
					                                              bind_data.num_rows);
					    if (window_rows > 0 &&
					        H5ReadShouldCreateCache(spec, window_rows, bind_data.num_rows, result->scan_batch_size)) {
						    state.cache = std::make_unique<RegularColumnCache>();
						    state.cache->window_rows = window_rows;

						    auto window_count = ComputeCacheWindowCount(window_rows, bind_data.num_rows);

						    auto base_type = GetBaseType(spec.column_type);
						    DispatchOnNumericType(base_type, [&](auto type_tag) {
							    using T = typename decltype(type_tag)::type;
							    for (idx_t window_idx = 0; window_idx < window_count; window_idx++) {
								    auto &window = state.cache->windows[window_idx];
								    auto buffer_elements = CheckedDatasetSizeProduct(window_rows, spec.elements_per_row,
								                                                     bind_data.filename, spec.path);
								    window.cache = std::vector<T>(buffer_elements);
							    }
						    });

						    cache_refresh_entries.push_back({local_idx,
						                                     TryGetDatasetReadOrderAddress(spec, state.dataset.get()),
						                                     cache_refresh_entries.size()});
					    }
				    }

				    // Store in dense array with LOCAL indexing
				    result->column_states.push_back(std::move(state));

			    } else if constexpr (std::is_same_v<T, ScalarColumnSpec>) {
				    if (spec.is_null_dataspace) {
					    result->column_states.push_back(ScalarColumnState {});
					    return;
				    }

				    // Scalar column - open dataset and cache value once
				    H5DatasetHandle dataset;
				    {
					    H5ErrorSuppressor suppress;
					    dataset = H5DatasetHandle(result->file, spec.path.c_str());
				    }

				    if (!dataset.is_valid()) {
					    throw IOException(FormatDatasetError("Failed to open dataset", bind_data.filename, spec.path));
				    }

				    ScalarColumnState scalar_state;
				    if (spec.string_h5_type) {
					    std::string value;
					    ReadHDF5Strings(dataset, *spec.string_h5_type, H5S_ALL, H5S_ALL, 1, bind_data.filename,
					                    spec.path, [&](idx_t, const std::string &str) { value = str; });
					    scalar_state.value = value;
				    } else {
					    auto base_type = GetBaseType(spec.column_type);
					    DispatchOnNumericType(base_type, [&](auto type_tag) {
						    using T = typename decltype(type_tag)::type;
						    T value {};
						    H5ErrorSuppressor suppress;
						    herr_t status =
						        H5Dread(dataset, GetNativeH5Type<T>(), H5S_ALL, H5S_ALL, H5P_DEFAULT, &value);
						    if (status < 0) {
							    throw IOException(FormatRemoteDatasetReadError(bind_data.filename, spec.path));
						    }
						    scalar_state.value = value;
					    });
				    }

				    // Store in dense array with LOCAL indexing
				    result->column_states.push_back(std::move(scalar_state));

			    } else if constexpr (std::is_same_v<T, RunEncodedColumnSpec>) {
				    // Run-encoded column - load boundaries and values using stored types from Bind
				    RunEncodedColumnState encoded_col;

				    // Open datasets (types were inspected in Bind phase) - RAII handles cleanup
				    H5DatasetHandle boundaries_ds;
				    H5DatasetHandle values_ds;
				    {
					    H5ErrorSuppressor suppress;
					    boundaries_ds = H5DatasetHandle(result->file, spec.boundaries_path.c_str());
					    if (!boundaries_ds.is_valid()) {
						    throw IOException(FormatDatasetError(string("Failed to open ") +
						                                             RunEncodingName(spec.encoding) + " " +
						                                             RunBoundaryName(spec.encoding) + " dataset",
						                                         bind_data.filename, spec.boundaries_path));
					    }

					    values_ds = H5DatasetHandle(result->file, spec.values_path.c_str());
					    if (!values_ds.is_valid()) {
						    throw IOException(FormatDatasetError(string("Failed to open ") +
						                                             RunEncodingName(spec.encoding) + " values dataset",
						                                         bind_data.filename, spec.values_path));
					    }
				    }

				    // Get array sizes
				    H5DataspaceHandle boundaries_space(boundaries_ds);
				    int boundaries_ndims = H5Sget_simple_extent_ndims(boundaries_space);
				    if (boundaries_ndims < 0) {
					    throw IOException(FormatDatasetError(string("Failed to get dimensions for ") +
					                                             RunEncodingName(spec.encoding) + " " +
					                                             RunBoundaryName(spec.encoding) + " dataset",
					                                         bind_data.filename, spec.boundaries_path));
				    }
				    if (boundaries_ndims != 1) {
					    throw IOException(FormatDatasetError(string(RunEncodingName(spec.encoding)) + " " +
					                                             RunBoundaryName(spec.encoding) +
					                                             " must be a 1-dimensional dataset",
					                                         bind_data.filename, spec.boundaries_path));
				    }
				    hssize_t num_runs_hssize = H5Sget_simple_extent_npoints(boundaries_space);

				    H5DataspaceHandle values_space(values_ds);
				    int values_ndims = H5Sget_simple_extent_ndims(values_space);
				    if (values_ndims < 0) {
					    throw IOException(FormatDatasetError(string("Failed to get dimensions for ") +
					                                             RunEncodingName(spec.encoding) + " values dataset",
					                                         bind_data.filename, spec.values_path));
				    }
				    if (values_ndims != 1) {
					    throw IOException(FormatDatasetError(string(RunEncodingName(spec.encoding)) +
					                                             " values must be a 1-dimensional dataset",
					                                         bind_data.filename, spec.values_path));
				    }
				    hssize_t num_values_hssize = H5Sget_simple_extent_npoints(values_space);

				    if (num_runs_hssize < 0) {
					    throw IOException(FormatDatasetError(string("Failed to get dataset size for ") +
					                                             RunEncodingName(spec.encoding) + " " +
					                                             RunBoundaryName(spec.encoding) + " dataset",
					                                         bind_data.filename, spec.boundaries_path));
				    }
				    if (num_values_hssize < 0) {
					    throw IOException(FormatDatasetError(string("Failed to get dataset size for ") +
					                                             RunEncodingName(spec.encoding) + " values dataset",
					                                         bind_data.filename, spec.values_path));
				    }

				    size_t num_runs = static_cast<size_t>(num_runs_hssize);
				    size_t num_values = static_cast<size_t>(num_values_hssize);

				    // Validate: boundaries and values must have same size
				    if (num_runs != num_values) {
					    throw IOException(FormatRunEncodedDatasetPairError(
					        string(RunEncodingName(spec.encoding)) + " " + RunBoundaryName(spec.encoding) +
					            " and values must have same size. Got " + std::to_string(num_runs) + " and " +
					            std::to_string(num_values),
					        bind_data.filename, spec));
				    }

				    encoded_col.run_starts = LoadRunBoundaries(bind_data.filename, spec, boundaries_ds, num_runs,
				                                               bind_data.num_rows, encoded_col.non_null_end);
				    encoded_col.values = LoadRunEncodedValues(bind_data.filename, spec, values_ds, num_values);

				    // Note: encoded column state is stateless (thread-safe)
				    // No runtime state initialization needed

				    // Store in dense array with LOCAL indexing
				    result->column_states.push_back(std::move(encoded_col));
			    } else if constexpr (std::is_same_v<T, IndexColumnSpec>) {
				    // Virtual index column - no HDF5 state required
				    result->column_states.push_back(IndexColumnState {});
			    }
		    },
		    col);
	}

	std::stable_sort(cache_refresh_entries.begin(), cache_refresh_entries.end(),
	                 [](const CacheRefreshOrderEntry &lhs, const CacheRefreshOrderEntry &rhs) {
		                 if (lhs.address.has_value() != rhs.address.has_value()) {
			                 return lhs.address.has_value();
		                 }
		                 if (lhs.address.has_value() && rhs.address.has_value() && *lhs.address != *rhs.address) {
			                 return *lhs.address < *rhs.address;
		                 }
		                 return lhs.original_order < rhs.original_order;
	                 });
	result->cache_refresh_order.reserve(cache_refresh_entries.size());
	for (const auto &entry : cache_refresh_entries) {
		result->cache_refresh_order.push_back(entry.local_idx);
	}

	// Compute row ranges based on claimed filters (from pushdown_complex_filter)
	// Group claimed filters by column
	unordered_map<idx_t, vector<ClaimedFilter>> filters_by_column;
	for (const auto &filter : bind_data.claimed_filters) {
		filters_by_column[filter.column_index].push_back(filter);
	}

	// If we have filters on run-encoded or index columns, compute row ranges
	if (!filters_by_column.empty()) {
		vector<RowRange> ranges = {{0, bind_data.num_rows}};

		for (const auto &[global_idx_raw, col_filters] : filters_by_column) {
			// Map global column index to local column_states index
			GlobalColumnIdx global_idx(global_idx_raw);
			vector<RowRange> col_ranges = BuildRangesForColumn(global_idx, col_filters, bind_data, *result);

			// Intersect ranges from this column with accumulated ranges
			ranges = IntersectRowRanges(ranges, col_ranges);
		}

		result->valid_row_ranges = std::move(ranges);
	} else {
		// No pushdown filters - all rows are valid
		result->valid_row_ranges.push_back({0, bind_data.num_rows});
	}

	result->position_done = AdjustPositionDoneForRanges(result->valid_row_ranges, 0);

	return result;
}

// Helper: Flip comparison for when constant is on left side (e.g., 10 < col becomes col > 10)
static ExpressionType FlipComparison(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_LESSTHAN:
		return ExpressionType::COMPARE_GREATERTHAN;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return ExpressionType::COMPARE_GREATERTHANOREQUALTO;
	case ExpressionType::COMPARE_GREATERTHAN:
		return ExpressionType::COMPARE_LESSTHAN;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ExpressionType::COMPARE_LESSTHANOREQUALTO;
	default:
		return type; // EQUAL stays EQUAL
	}
}

static idx_t H5ReadDecimalDigitCount(idx_t value) {
	idx_t digits = 1;
	while (value >= 10) {
		value /= 10;
		digits++;
	}
	return digits;
}

static bool H5ReadIndexFitsIntegralCast(LogicalTypeId target_type, idx_t max_index) {
	switch (target_type) {
	case LogicalTypeId::TINYINT:
		return max_index <= static_cast<idx_t>(std::numeric_limits<int8_t>::max());
	case LogicalTypeId::SMALLINT:
		return max_index <= static_cast<idx_t>(std::numeric_limits<int16_t>::max());
	case LogicalTypeId::INTEGER:
		return max_index <= static_cast<idx_t>(std::numeric_limits<int32_t>::max());
	case LogicalTypeId::BIGINT:
		return max_index <= static_cast<idx_t>(std::numeric_limits<int64_t>::max());
	case LogicalTypeId::HUGEINT:
		return true;
	case LogicalTypeId::UTINYINT:
		return max_index <= static_cast<idx_t>(std::numeric_limits<uint8_t>::max());
	case LogicalTypeId::USMALLINT:
		return max_index <= static_cast<idx_t>(std::numeric_limits<uint16_t>::max());
	case LogicalTypeId::UINTEGER:
		return max_index <= static_cast<idx_t>(std::numeric_limits<uint32_t>::max());
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
		return true;
	default:
		return false;
	}
}

static bool H5ReadIndexFitsDecimalCast(const LogicalType &target_type, idx_t max_index) {
	uint8_t width;
	uint8_t scale;
	if (!target_type.GetDecimalProperties(width, scale)) {
		return false;
	}
	if (scale > width) {
		return false;
	}
	return H5ReadDecimalDigitCount(max_index) <= static_cast<idx_t>(width - scale);
}

static bool H5ReadIndexCastTargetIsMonotone(const LogicalType &target_type, idx_t max_index) {
	switch (target_type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
		return H5ReadIndexFitsIntegralCast(target_type.id(), max_index);
	case LogicalTypeId::DECIMAL:
		return H5ReadIndexFitsDecimalCast(target_type, max_index);
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return true;
	default:
		return false;
	}
}

static bool H5ReadIndexComparisonTypeIsMonotone(const PushdownColumnRef &ref, idx_t max_index) {
	if (max_index > static_cast<idx_t>(std::numeric_limits<int64_t>::max())) {
		return false;
	}
	return H5ReadIndexCastTargetIsMonotone(ref.comparison_type, max_index);
}

static bool ExtractPushdownColumnRef(const Expression &expr, PushdownColumnRef &result) {
	result.column_ref = nullptr;
	result.comparison_type = expr.return_type;

	const Expression *current = &expr;
	if (current->expression_class == ExpressionClass::BOUND_CAST) {
		auto &cast = current->Cast<BoundCastExpression>();
		if (cast.try_cast || cast.child->expression_class == ExpressionClass::BOUND_CAST) {
			return false;
		}
		current = cast.child.get();
	}
	if (current->expression_class != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	result.column_ref = &current->Cast<BoundColumnRefExpression>();
	return true;
}

template <typename TableIndexT>
static bool TryResolvePushdownColumn(const PushdownColumnRef &ref, const TableIndexT &table_index,
                                     const unordered_map<idx_t, idx_t> &get_to_bind_map,
                                     const unordered_set<idx_t> &pushdown_columns, idx_t &bind_data_col_idx) {
	if (!ref.column_ref || ref.column_ref->binding.table_index != table_index) {
		return false;
	}
	auto it = get_to_bind_map.find(ref.column_ref->binding.column_index);
	if (it == get_to_bind_map.end() || pushdown_columns.count(it->second) == 0) {
		return false;
	}
	bind_data_col_idx = it->second;
	return true;
}

static bool H5ReadCanClaimComparison(ExpressionType comparison) {
	switch (comparison) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return true;
	default:
		return false;
	}
}

static bool H5ReadValueCanCastTo(const Value &value, const LogicalType &target_type) {
	Value cast_value = value;
	return cast_value.DefaultTryCastAs(target_type, true);
}

static bool H5ReadCanClaimPushdownFilter(const ColumnSpec &column, const PushdownColumnRef &ref, const Value &constant,
                                         idx_t max_index) {
	if (!std::holds_alternative<IndexColumnSpec>(column)) {
		return true;
	}
	return !constant.IsNull() && H5ReadIndexComparisonTypeIsMonotone(ref, max_index) &&
	       H5ReadValueCanCastTo(constant, ref.comparison_type);
}

static bool H5ReadCanClaimPushdownBetween(const ColumnSpec &column, const PushdownColumnRef &ref, const Value &lower,
                                          const Value &upper, idx_t max_index) {
	if (!std::holds_alternative<IndexColumnSpec>(column)) {
		return true;
	}
	return !lower.IsNull() && !upper.IsNull() && H5ReadIndexComparisonTypeIsMonotone(ref, max_index) &&
	       H5ReadValueCanCastTo(lower, ref.comparison_type) && H5ReadValueCanCastTo(upper, ref.comparison_type);
}

static void H5ReadAddClaimedFilter(vector<ClaimedFilter> &claimed, idx_t column_index, ExpressionType comparison,
                                   const Value &constant, const LogicalType &comparison_type) {
	ClaimedFilter filter;
	filter.column_index = column_index;
	filter.comparison = comparison;
	filter.constant = constant;
	filter.comparison_type = comparison_type;
	claimed.push_back(std::move(filter));
}

// Helper: Try to claim a filter on a run-encoded or index column
template <typename TableIndexT>
static bool TryClaimPushdownFilter(const unique_ptr<Expression> &expr, const TableIndexT &table_index,
                                   const unordered_map<idx_t, idx_t> &get_to_bind_map,
                                   const unordered_set<idx_t> &pushdown_columns, const vector<ColumnSpec> &columns,
                                   idx_t max_index, vector<ClaimedFilter> &claimed) {
	// Handle comparison expressions: col > 10, col = 20, 10 < col, etc.
	if (expr->expression_class == ExpressionClass::BOUND_COMPARISON) {
		auto &comp = expr->Cast<BoundComparisonExpression>();

		const BoundConstantExpression *constant = nullptr;
		PushdownColumnRef ref;
		bool found_colref = false;
		bool need_flip = false;

		// Determine which side is the column and which is the constant
		if (comp.right->expression_class == ExpressionClass::BOUND_CONSTANT) {
			found_colref = ExtractPushdownColumnRef(*comp.left, ref);
			constant = &comp.right->Cast<BoundConstantExpression>();
			need_flip = false;
		} else if (comp.left->expression_class == ExpressionClass::BOUND_CONSTANT) {
			found_colref = ExtractPushdownColumnRef(*comp.right, ref);
			constant = &comp.left->Cast<BoundConstantExpression>();
			need_flip = true;
		}

		idx_t bind_data_col_idx;
		ExpressionType comparison = need_flip ? FlipComparison(comp.type) : comp.type;
		if (!found_colref || !constant || !H5ReadCanClaimComparison(comparison) ||
		    !TryResolvePushdownColumn(ref, table_index, get_to_bind_map, pushdown_columns, bind_data_col_idx) ||
		    !H5ReadCanClaimPushdownFilter(columns[bind_data_col_idx], ref, constant->value, max_index)) {
			return false;
		}
		H5ReadAddClaimedFilter(claimed, bind_data_col_idx, comparison, constant->value, ref.comparison_type);
		return true;
	}

	// Handle BETWEEN: col BETWEEN lower AND upper
	if (expr->expression_class == ExpressionClass::BOUND_BETWEEN) {
		auto &between = expr->Cast<BoundBetweenExpression>();
		PushdownColumnRef ref;
		auto found_colref = ExtractPushdownColumnRef(*between.input, ref);

		idx_t bind_data_col_idx;
		if (!found_colref || between.lower->expression_class != ExpressionClass::BOUND_CONSTANT ||
		    between.upper->expression_class != ExpressionClass::BOUND_CONSTANT ||
		    !TryResolvePushdownColumn(ref, table_index, get_to_bind_map, pushdown_columns, bind_data_col_idx)) {
			return false;
		}

		auto &lower_const = between.lower->Cast<BoundConstantExpression>();
		auto &upper_const = between.upper->Cast<BoundConstantExpression>();
		if (!H5ReadCanClaimPushdownBetween(columns[bind_data_col_idx], ref, lower_const.value, upper_const.value,
		                                   max_index)) {
			return false;
		}
		H5ReadAddClaimedFilter(claimed, bind_data_col_idx, between.LowerComparisonType(), lower_const.value,
		                       ref.comparison_type);
		H5ReadAddClaimedFilter(claimed, bind_data_col_idx, between.UpperComparisonType(), upper_const.value,
		                       ref.comparison_type);
		return true;
	}

	// Handle CONJUNCTION_AND (for other compound filters)
	if (expr->expression_class == ExpressionClass::BOUND_CONJUNCTION) {
		auto &conj = expr->Cast<BoundConjunctionExpression>();

		if (conj.type == ExpressionType::CONJUNCTION_AND) {
			// Try to claim pushdown-eligible filters from all children of a flattened AND
			vector<ClaimedFilter> temp_claimed;
			bool claimed_any = false;
			for (const auto &child : conj.children) {
				claimed_any |= TryClaimPushdownFilter(child, table_index, get_to_bind_map, pushdown_columns, columns,
				                                      max_index, temp_claimed);
			}

			// If we claimed any pushdown filters, add them to optimize I/O and return true
			if (claimed_any) {
				claimed.insert(claimed.end(), temp_claimed.begin(), temp_claimed.end());
				return true; // Indicate we claimed something
			}

			// No run-encoded filters in this conjunction
			return false;
		}
	}

	return false; // Can't handle this filter
}

// Pushdown complex filter callback
// This is called during query optimization to determine which filters we can handle
static void H5ReadPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                        vector<unique_ptr<Expression>> &filters) {
	auto &bind_data = bind_data_p->Cast<H5ReadBindData>();
	const auto &columns = GetCanonicalColumns(bind_data);

	// Build set of pushdown-eligible column indices (run-encoded or index)
	unordered_set<idx_t> pushdown_column_indices;
	for (idx_t i = 0; i < columns.size(); i++) {
		if (std::holds_alternative<RunEncodedColumnSpec>(columns[i]) ||
		    std::holds_alternative<IndexColumnSpec>(columns[i])) {
			pushdown_column_indices.insert(i);
		}
	}

	// Map LogicalGet column indices to bind_data column indices using column_ids
	// This is structural (not name-based) and works correctly with projections/aliases
	unordered_map<idx_t, idx_t> get_to_bind_map; // get column idx -> bind_data column idx
	const auto &column_ids = get.GetColumnIds();
	if (column_ids.empty()) {
		for (idx_t i = 0; i < columns.size(); i++) {
			get_to_bind_map[i] = i;
		}
	} else {
		for (idx_t i = 0; i < column_ids.size(); i++) {
			auto column_id = column_ids[i].GetPrimaryIndex();
			if (!H5ReadIsDataColumn(bind_data, column_id)) {
				continue;
			}
			get_to_bind_map[i] = column_id;
		}
	}

	const auto &table_index = get.table_index;
	idx_t max_num_rows = 0;
	for (const auto &file_bind_data : bind_data.file_bind_data) {
		max_num_rows = MaxValue<idx_t>(max_num_rows, file_bind_data.num_rows);
	}
	const auto max_index = max_num_rows == 0 ? 0 : max_num_rows - 1;

	// Claim filters for I/O optimization (but keep them in filter list for post-scan)
	// DuckDB will apply all filters after scan to ensure correctness (defensive approach)
	for (const auto &expr : filters) {
		TryClaimPushdownFilter(expr, table_index, get_to_bind_map, pushdown_column_indices, columns, max_index,
		                       bind_data.claimed_filters);
	}
}

// Scan function - read data chunks

// Helper structure for range selection results
struct RangeSelection {
	bool has_data;  // False if no more data to read
	idx_t position; // Starting position to read from
	idx_t to_read;  // Number of rows to read
};

static RangeSelection NextRangeFrom(const std::vector<RowRange> &valid_row_ranges, idx_t position, idx_t position_end,
                                    idx_t batch_size) {
	auto it = FindRangeForPosition(valid_row_ranges, position);
	if (it != valid_row_ranges.end()) {
		idx_t start = position < it->start_row ? it->start_row : position;
		if (start >= position_end) {
			return {false, 0, 0};
		}
		idx_t remains = MinValue<idx_t>(batch_size, MinValue<idx_t>(it->end_row, position_end) - start);
		return {true, start, remains};
	}
	return {false, 0, 0}; // no range after position
}

static RangeSelection NextRangeFrom(const std::vector<RowRange> &valid_row_ranges, idx_t position) {
	return NextRangeFrom(valid_row_ranges, position, NumericLimits<idx_t>::Maximum(), NumericLimits<idx_t>::Maximum());
}

static RangeSelection ClaimNextRange(H5ReadGlobalState &gstate, idx_t num_rows) {
	std::lock_guard<std::mutex> lock(gstate.range_selection_mutex);
	auto range = NextRangeFrom(gstate.valid_row_ranges, gstate.position, num_rows, gstate.scan_batch_size);
	if (!range.has_data) {
		return {false, 0, 0};
	}
	gstate.position = range.position + range.to_read;
	return range;
}

static void MarkRangeComplete(H5ReadGlobalState &gstate, idx_t position, idx_t count) {
	std::lock_guard<std::mutex> lock(gstate.range_selection_mutex);
	auto completed_through = gstate.position_done.load(std::memory_order_acquire);
	auto scan_end = position + count;
	if (position != completed_through) {
		gstate.completed_ranges[position] = scan_end;
		return;
	}

	completed_through = scan_end;
	while (true) {
		completed_through = AdjustPositionDoneForRanges(gstate.valid_row_ranges, completed_through);
		auto it = gstate.completed_ranges.find(completed_through);
		if (it == gstate.completed_ranges.end()) {
			break;
		}
		completed_through = it->second;
		gstate.completed_ranges.erase(it);
	}
	gstate.position_done.store(completed_through, std::memory_order_release);
}

// Helper function to scan a run-encoded column
static void ScanRunEncodedColumn(const RunEncodedColumnSpec &spec, RunEncodedColumnState &state, Vector &result_vector,
                                 idx_t position, idx_t to_read) {
	// Dispatch once per chunk, not per row
	DispatchOnDuckDBType(spec.column_type, [&](auto type_tag) {
		using T = typename decltype(type_tag)::type;

		// Access typed vector directly (no Value overhead)
		const auto &typed_values = std::get<std::vector<T>>(state.values);
		D_ASSERT(state.run_starts.size() == typed_values.size());
		if (state.run_starts.empty()) {
			// Empty run encoding: emit NULLs for the requested rows
			result_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result_vector, true);
			return;
		}
		if (position >= state.non_null_end) {
			result_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result_vector, true);
			return;
		}

		idx_t result_offset = 0;

		// Leading NULL segment if first run starts after 0
		if (position < state.run_starts[0]) {
			result_offset = std::min<idx_t>(to_read, state.run_starts[0] - position);
			if (result_offset == to_read) {
				result_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(result_vector, true);
				return;
			}
			position += result_offset;
			to_read -= result_offset;
		}
		if (position >= state.non_null_end) {
			result_vector.SetVectorType(VectorType::FLAT_VECTOR);
			for (idx_t i = 0; i < result_offset + to_read; i++) {
				FlatVector::SetNull(result_vector, i, true);
			}
			return;
		}

		idx_t null_suffix = 0;
		idx_t non_null_remaining = state.non_null_end - position;
		if (to_read > non_null_remaining) {
			null_suffix = to_read - non_null_remaining;
			to_read = non_null_remaining;
		}

		auto it = std::upper_bound(state.run_starts.begin(), state.run_starts.end(), position);
		idx_t current_run = (it - state.run_starts.begin()) - 1;
		idx_t next_run_start =
		    (current_run + 1 < state.run_starts.size()) ? state.run_starts[current_run + 1] : state.non_null_end;

		// OPTIMIZATION: Check if entire chunk belongs to single run
		// With avg run length ~10k and chunk size 2048, this is true ~83% of the time!
		idx_t rows_in_current_run = next_run_start - position;
		if (result_offset == 0 && null_suffix == 0 && rows_in_current_run >= to_read) {
			// Entire chunk is one value - emit CONSTANT_VECTOR (no fill loop needed!)
			result_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
			const T &run_value = typed_values[current_run];

			if constexpr (std::is_same_v<T, string>) {
				auto result_data = ConstantVector::GetData<string_t>(result_vector);
				result_data[0] = StringVector::AddString(result_vector, run_value);
			} else {
				auto result_data = ConstantVector::GetData<T>(result_vector);
				result_data[0] = run_value;
			}
			return;
		}

		// Fall back to batched fill when chunk spans multiple runs
		result_vector.SetVectorType(VectorType::FLAT_VECTOR);
		if (result_offset > 0) {
			for (idx_t i = 0; i < result_offset; i++) {
				FlatVector::SetNull(result_vector, i, true);
			}
		}
		idx_t i = 0;
		while (i < to_read) {
			// Calculate current row position
			idx_t current_row = position + i;

			// Calculate how many rows belong to current run
			rows_in_current_run = next_run_start - current_row;
			idx_t rows_to_fill = std::min(rows_in_current_run, to_read - i);

			// Get the value for this run
			const T &run_value = typed_values[current_run];

			// Tight loop: fill all rows with same value (no conditionals!)
			if constexpr (std::is_same_v<T, string>) {
				// VARCHAR: need to call StringVector::AddString for each
				auto result_data = FlatVector::GetData<string_t>(result_vector);
				for (idx_t j = 0; j < rows_to_fill; j++) {
					result_data[result_offset + i + j] = StringVector::AddString(result_vector, run_value);
				}
			} else {
				// Numeric types: direct assignment - compiler can vectorize this!
				auto result_data = FlatVector::GetData<T>(result_vector);
				for (idx_t j = 0; j < rows_to_fill; j++) {
					result_data[result_offset + i + j] = run_value;
				}
			}

			i += rows_to_fill;

			// If we've exhausted current run and there's more to read, advance to next run
			if (i < to_read) {
				current_run++;
				next_run_start = (current_run + 1 < state.run_starts.size()) ? state.run_starts[current_run + 1]
				                                                             : state.non_null_end;
			}
		}
		for (idx_t j = 0; j < null_suffix; j++) {
			FlatVector::SetNull(result_vector, result_offset + to_read + j, true);
		}
	});
}

// ==================== Cache Window Helpers ====================

// Helper: Read data from HDF5 into typed cache buffer
static void ReadIntoTypedCache(CacheWindow::CacheStorage &cache, hid_t dataset_id, hid_t file_space_id,
                               idx_t dataset_row_start, idx_t rows_to_read, const RegularColumnSpec &spec,
                               const string &filename) {
	auto base_type = GetBaseType(spec.column_type);
	DispatchOnNumericType(base_type, [&](auto type_tag) {
		using T = typename decltype(type_tag)::type;
		auto &typed_cache = std::get<std::vector<T>>(cache);

		// Lock for all HDF5 operations (not thread-safe)
		std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);

		H5DataspaceHandle mem_space = CreateMemspaceAndSelect(file_space_id, spec, dataset_row_start, rows_to_read);

		H5ErrorSuppressor suppress;
		herr_t status =
		    H5Dread(dataset_id, GetNativeH5Type<T>(), mem_space, file_space_id, H5P_DEFAULT, typed_cache.data());
		if (status < 0) {
			throw IOException(FormatRemoteDatasetReadError(filename, spec.path));
		}
	});
}

// Helper: Copy data from typed cache to result vector
static void CopyFromTypedCache(const CacheWindow::CacheStorage &cache, idx_t buffer_offset_rows, idx_t rows_to_copy,
                               Vector &result_vector, idx_t result_offset_rows, LogicalType column_type,
                               idx_t elements_per_row) {
	DispatchOnNumericType(column_type, [&](auto type_tag) {
		using T = typename decltype(type_tag)::type;
		const auto &typed_cache = std::get<std::vector<T>>(cache);

		// Get result data pointer
		auto result_data = FlatVector::GetData<T>(result_vector);

		// Copy from cache to result
		idx_t buffer_offset = buffer_offset_rows * elements_per_row;
		idx_t result_offset = result_offset_rows * elements_per_row;
		idx_t elements_to_copy = rows_to_copy * elements_per_row;
		std::memcpy(result_data + result_offset, typed_cache.data() + buffer_offset, elements_to_copy * sizeof(T));
	});
}

static void TryLoadCacheWindows(RegularColumnCache &cache, hid_t dataset_id, hid_t file_space_id,
                                const std::vector<RowRange> &valid_row_ranges, std::atomic<idx_t> &position_done,
                                idx_t total_rows, const RegularColumnSpec &spec, const string &filename) {
	auto window_count = ComputeCacheWindowCount(cache.window_rows, total_rows);
	idx_t max_end_row = 0;
	for (idx_t i = 0; i < window_count; i++) {
		auto &window = cache.windows[i];
		auto end_row = window.end_row.load(std::memory_order_acquire);
		max_end_row = end_row > max_end_row ? end_row : max_end_row;
	}
	auto position_done_value = position_done.load(std::memory_order_acquire);
	for (idx_t i = 0; i < window_count; i++) {
		auto &window = cache.windows[i];
		if (window.end_row.load(std::memory_order_acquire) <= position_done_value) {
			// Every row in this window has been returned or skipped, so it can be reused.
			auto next_range = NextRangeFrom(valid_row_ranges, max_end_row);
			if (next_range.has_data) {
				idx_t rows_to_load = std::min(cache.window_rows, total_rows - next_range.position);
				ReadIntoTypedCache(window.cache, dataset_id, file_space_id, next_range.position, rows_to_load, spec,
				                   filename);

				idx_t new_end = next_range.position + cache.window_rows;
				window.end_row.store(new_end, std::memory_order_release);

				max_end_row = new_end;
			}
		}
	}
}

static void FinishCacheFetch(H5ReadGlobalState &gstate) {
	gstate.someone_is_fetching.store(false);
	gstate.someone_is_fetching.notify_all();
}

static void TryRefreshCache(H5ReadGlobalState &gstate, const H5ReadSingleFileBindView &bind_data) {
	bool expected = false;
	if (gstate.someone_is_fetching.compare_exchange_strong(expected, true)) {
		// Exactly one thread refreshes cache windows at a time. Other threads return
		// immediately here and only block later if the windows covering their read
		// range are still not available.
		try {
			for (auto local_idx : gstate.cache_refresh_order) {
				GlobalColumnIdx global_idx = GetGlobalIdx(gstate, local_idx);
				const auto &spec = std::get<RegularColumnSpec>(bind_data.columns[global_idx]);
				auto &state = std::get<RegularColumnState>(gstate.column_states[local_idx]);
				D_ASSERT(state.cache);

				TryLoadCacheWindows(*state.cache, state.dataset.get(), state.file_space.get(), gstate.valid_row_ranges,
				                    gstate.position_done, bind_data.num_rows, spec, bind_data.filename);
			}
		} catch (...) {
			FinishCacheFetch(gstate);
			throw;
		}
		// Done loading - release the flag so another thread can load next time
		FinishCacheFetch(gstate);
	}
}

// Helper function to scan a regular dataset column
static void ScanRegularColumn(ClientContext &context, const RegularColumnSpec &spec, RegularColumnState &state,
                              Vector &result_vector, idx_t position, idx_t to_read,
                              const H5ReadSingleFileBindView &bind_data, H5ReadGlobalState &gstate) {
	ThrowIfInterrupted(context);
	auto base_type = GetBaseType(spec.column_type);
	auto &target_vector = PrepareRegularResultVector(result_vector, spec, to_read, bind_data.filename);

	if (state.cache) {
		auto &cache = *state.cache;
		auto window_count = ComputeCacheWindowCount(cache.window_rows, bind_data.num_rows);

		auto *window1 = &cache.windows[0];
		CacheWindow *window2 = window_count > 1 ? &cache.windows[1] : nullptr;
		for (;;) {
			ThrowIfInterrupted(context);

			TryRefreshCache(gstate, bind_data);

			idx_t end1 = window1->end_row.load(std::memory_order_acquire);
			idx_t end2 = window2 ? window2->end_row.load(std::memory_order_acquire) : end1;

			if (window2 && end1 > end2) {
				std::swap(window1, window2);
				std::swap(end1, end2);
			}

			if (position + to_read <= end2) {
				break;
			}

			if (gstate.someone_is_fetching.load(std::memory_order_acquire)) {
				gstate.someone_is_fetching.wait(true, std::memory_order_relaxed);
			}
		}

		// Copy data from windows that overlap our read range [position, position + to_read)
		for (auto *window : {window1, window2}) {
			if (!window) {
				continue;
			}
			idx_t window_end = window->end_row.load(std::memory_order_acquire);
			idx_t window_start = (window_end > cache.window_rows) ? (window_end - cache.window_rows) : 0;

			if (window_start < position + to_read && window_end > position) {
				idx_t overlap_start = MaxValue<idx_t>(window_start, position);
				idx_t overlap_end = MinValue<idx_t>(window_end, position + to_read);
				idx_t overlap_size = overlap_end - overlap_start;

				idx_t window_offset = overlap_start - window_start;
				idx_t result_offset = overlap_start - position;
				D_ASSERT(window_offset + overlap_size <= cache.window_rows);

				CopyFromTypedCache(window->cache, window_offset, overlap_size, target_vector, result_offset, base_type,
				                   spec.elements_per_row);
			}
		}

		return; // Done with cached read
	}

	std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);

	// Non-cached path: direct HDF5 read for uncached data types/layouts.
	// Access RAII-wrapped handles from state
	hid_t dataset_id = state.dataset.get();
	hid_t file_space = state.file_space.get();

	// Create memory dataspace for reading
	H5DataspaceHandle mem_space = CreateMemspaceAndSelect(file_space, spec, position, to_read);

	// Read data based on type
	if (spec.is_string) {
		D_ASSERT(spec.string_h5_type.has_value());
		// Handle string data using helper
		ReadHDF5Strings(dataset_id, *spec.string_h5_type, mem_space, file_space, to_read, bind_data.filename, spec.path,
		                [&](idx_t i, const std::string &str) {
			                FlatVector::GetData<string_t>(target_vector)[i] =
			                    StringVector::AddString(target_vector, str);
		                });

	} else {
		// Handle numeric data
		H5ErrorSuppressor suppress;
		herr_t status = DispatchOnNumericType(base_type, [&](auto type_tag) {
			using T = typename decltype(type_tag)::type;
			void *child_data = FlatVector::GetData<T>(target_vector);
			return H5Dread(dataset_id, GetNativeH5Type<T>(), mem_space, file_space, H5P_DEFAULT, child_data);
		});

		if (status < 0) {
			throw IOException(FormatRemoteDatasetReadError(bind_data.filename, spec.path));
		}
	}

	// Note: file_space is cached and will be closed in destructor
}

// Helper function to scan a scalar dataset column (broadcast cached value)
static void ScanScalarColumn(const ScalarColumnState &state, Vector &result_vector, idx_t to_read) {
	if (to_read == 0) {
		return;
	}

	result_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
	std::visit(
	    [&](const auto &value) {
		    using T = std::decay_t<decltype(value)>;
		    if constexpr (std::is_same_v<T, std::monostate>) {
			    ConstantVector::SetNull(result_vector, true);
		    } else if constexpr (std::is_same_v<T, string>) {
			    ConstantVector::GetData<string_t>(result_vector)[0] = StringVector::AddString(result_vector, value);
		    } else {
			    ConstantVector::GetData<T>(result_vector)[0] = value;
		    }
	    },
	    state.value);
}

static void H5ReadSingleFileScan(ClientContext &context, const H5ReadSingleFileBindView &bind_data,
                                 H5ReadGlobalState &gstate, DataChunk &output) {
	ThrowIfInterrupted(context);

	auto range_selection = ClaimNextRange(gstate, bind_data.num_rows);
	if (!range_selection.has_data) {
		output.SetCardinality(0);
		return;
	}

	idx_t position = range_selection.position;
	idx_t to_read = range_selection.to_read;

	// Process only scanned columns (projection pushdown)
	// Uses LOCAL indexing - both output.data and column_states are indexed [0, 1, 2...]
	for (idx_t i = 0; i < GetNumScannedColumns(gstate); i++) {
		LocalColumnIdx local_idx(i);
		GlobalColumnIdx global_idx = GetGlobalIdx(gstate, local_idx);

		auto &result_vector = output.data[gstate.output_column_positions[i]];
		const auto &col_spec = bind_data.columns[global_idx]; // Global schema
		auto &col_state = gstate.column_states[local_idx];    // Local (dense) state

		// Use variant visiting to dispatch based on column type
		std::visit(
		    [&](auto &&spec, auto &&state) {
			    using SpecT = std::decay_t<decltype(spec)>;
			    using StateT = std::decay_t<decltype(state)>;

			    if constexpr (std::is_same_v<SpecT, RunEncodedColumnSpec> &&
			                  std::is_same_v<StateT, RunEncodedColumnState>) {
				    // Run-encoded column - call helper function
				    ScanRunEncodedColumn(spec, state, result_vector, position, to_read);

			    } else if constexpr (std::is_same_v<SpecT, ScalarColumnSpec> &&
			                         std::is_same_v<StateT, ScalarColumnState>) {
				    // Scalar dataset - broadcast cached value
				    ScanScalarColumn(state, result_vector, to_read);

			    } else if constexpr (std::is_same_v<SpecT, RegularColumnSpec> &&
			                         std::is_same_v<StateT, RegularColumnState>) {
				    // Regular dataset - call helper function
				    ScanRegularColumn(context, spec, state, result_vector, position, to_read, bind_data, gstate);
			    } else if constexpr (std::is_same_v<SpecT, IndexColumnSpec> &&
			                         std::is_same_v<StateT, IndexColumnState>) {
				    // Virtual index column - sequence vector
				    result_vector.Sequence(static_cast<int64_t>(position), 1, to_read);
			    }
		    },
		    col_spec, col_state);
	}

	output.SetCardinality(to_read);
	if (!gstate.cache_refresh_order.empty()) {
		MarkRangeComplete(gstate, position, to_read);
	}
}

static void SetCurrentH5ReadFile(ClientContext &context, const H5ReadBindData &bind_data,
                                 H5ReadMultiFileGlobalState &gstate, idx_t file_idx) {
	gstate.current_file = InitSingleH5ReadState(context, GetSingleFileBindView(bind_data, file_idx),
	                                            gstate.data_column_ids, gstate.data_output_column_positions);
	gstate.current_file_idx = file_idx;
}

static void AttachLocalStateToCurrentFile(const H5ReadMultiFileGlobalState &gstate, H5ReadMultiFileLocalState &lstate) {
	D_ASSERT(gstate.current_file);
	lstate.file = gstate.current_file;
	lstate.file_idx = gstate.current_file_idx;
}

static void AdvanceCurrentH5ReadFile(ClientContext &context, const H5ReadBindData &bind_data,
                                     H5ReadMultiFileGlobalState &gstate, idx_t exhausted_file_idx) {
	if (gstate.current_file_idx != exhausted_file_idx) {
		return;
	}

	auto next_file_idx = exhausted_file_idx + 1;
	if (next_file_idx >= bind_data.file_bind_data.size()) {
		gstate.current_file.reset();
		gstate.current_file_idx = next_file_idx;
		return;
	}

	SetCurrentH5ReadFile(context, bind_data, gstate, next_file_idx);
}

// Init function - initialize the first file in the multi-file scan wrapper.
static unique_ptr<GlobalTableFunctionState> H5ReadInit(ClientContext &context, TableFunctionInitInput &input) {
	ThrowIfInterrupted(context);
	auto &bind_data = input.bind_data->Cast<H5ReadBindData>();
	auto result = make_uniq<H5ReadMultiFileGlobalState>();
	BuildH5ReadProjectionLayout(bind_data, input.column_ids, result->data_column_ids,
	                            result->data_output_column_positions, result->filename_output_positions,
	                            result->empty_output_positions);
	D_ASSERT(!bind_data.file_bind_data.empty());
	SetCurrentH5ReadFile(context, bind_data, *result, 0);
	return result;
}

static unique_ptr<LocalTableFunctionState> H5ReadInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                           GlobalTableFunctionState *global_state) {
	return make_uniq<H5ReadMultiFileLocalState>();
}

static void H5ReadPopulateFilenameColumns(const H5ReadBindData &bind_data, idx_t file_idx,
                                          const H5ReadMultiFileGlobalState &gstate, DataChunk &output) {
	if (output.size() == 0) {
		return;
	}
	if (gstate.filename_output_positions.empty()) {
		return;
	}
	auto &filename = bind_data.file_bind_data[file_idx].filename;
	for (auto output_idx : gstate.filename_output_positions) {
		auto &vector = output.data[output_idx];
		vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::GetData<string_t>(vector)[0] = StringVector::AddString(vector, filename);
	}
}

static void H5ReadPopulateEmptyColumns(const H5ReadMultiFileGlobalState &gstate, DataChunk &output) {
	if (output.size() == 0) {
		return;
	}
	for (auto output_idx : gstate.empty_output_positions) {
		auto &vector = output.data[output_idx];
		vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(vector, true);
	}
}

static void H5ReadScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	ThrowIfInterrupted(context);
	auto &bind_data = data.bind_data->Cast<H5ReadBindData>();
	auto &gstate = data.global_state->Cast<H5ReadMultiFileGlobalState>();
	auto &lstate = data.local_state->Cast<H5ReadMultiFileLocalState>();

	// A local scan state stays attached to one file across repeated scan calls.
	// When that file reaches EOF, it advances the current file if nobody else
	// has already done so. Other threads that still hold older file states can
	// finish their claimed ranges while new threads move on.
	while (true) {
		if (!lstate.file) {
			std::lock_guard<std::mutex> lock(gstate.current_file_lock);
			if (gstate.current_file_idx >= bind_data.file_bind_data.size()) {
				output.SetCardinality(0);
				return;
			}
			AttachLocalStateToCurrentFile(gstate, lstate);
		}

		auto file_idx = lstate.file_idx;
		auto file = lstate.file;
		H5ReadSingleFileScan(context, GetSingleFileBindView(bind_data, file_idx), *file, output);

		if (output.size() > 0) {
			H5ReadPopulateFilenameColumns(bind_data, file_idx, gstate, output);
			H5ReadPopulateEmptyColumns(gstate, output);
			return;
		}

		lstate.file.reset();

		{
			std::lock_guard<std::mutex> lock(gstate.current_file_lock);
			AdvanceCurrentH5ReadFile(context, bind_data, gstate, file_idx);
		}
	}
}

// ==================== h5_rse/h5_ree Scalar Functions ====================

static void H5RunEncodingFunction(DataChunk &args, Vector &result, RunEncodingKind encoding) {
	auto &boundaries_vec = args.data[0];
	auto &values_vec = args.data[1];

	UnifiedVectorFormat boundaries_data;
	UnifiedVectorFormat values_data;
	boundaries_vec.ToUnifiedFormat(args.size(), boundaries_data);
	values_vec.ToUnifiedFormat(args.size(), values_data);

	auto boundaries_ptr = UnifiedVectorFormat::GetData<string_t>(boundaries_data);
	auto values_ptr = UnifiedVectorFormat::GetData<string_t>(values_data);

	auto &children = StructVector::GetEntries(result);
	D_ASSERT(children.size() == 3);
	auto &encoding_child = GetStructChild(children[0]);
	auto &boundaries_child = GetStructChild(children[1]);
	auto &values_child = GetStructChild(children[2]);

	for (idx_t i = 0; i < args.size(); i++) {
		auto boundaries_idx = boundaries_data.sel->get_index(i);
		auto values_idx = values_data.sel->get_index(i);

		FlatVector::GetData<string_t>(encoding_child)[i] =
		    StringVector::AddString(encoding_child, RunEncodingTag(encoding));
		FlatVector::GetData<string_t>(boundaries_child)[i] =
		    StringVector::AddString(boundaries_child, boundaries_ptr[boundaries_idx]);
		FlatVector::GetData<string_t>(values_child)[i] = StringVector::AddString(values_child, values_ptr[values_idx]);
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
	if (args.size() == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void H5RseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	H5RunEncodingFunction(args, result, RunEncodingKind::START);
}

static void H5ReeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	H5RunEncodingFunction(args, result, RunEncodingKind::END);
}

void RegisterH5RseFunction(ExtensionLoader &loader) {
	child_list_t<LogicalType> struct_children = {
	    {"encoding", LogicalType::VARCHAR}, {"run_starts", LogicalType::VARCHAR}, {"values", LogicalType::VARCHAR}};

	auto h5_rse = ScalarFunction("h5_rse", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                             LogicalType::STRUCT(struct_children), H5RseFunction);
	CreateScalarFunctionInfo info(std::move(h5_rse));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	info.descriptions.push_back(
	    H5FunctionDescription({LogicalType::VARCHAR, LogicalType::VARCHAR}, {"run_starts_path", "values_path"},
	                          "Creates a run-start encoded column definition for h5_read().",
	                          {"FROM h5_read('data.h5', '/time', h5_rse('/state_run_starts', '/state_values'))"}));
	loader.RegisterFunction(std::move(info));
}

void RegisterH5ReeFunction(ExtensionLoader &loader) {
	child_list_t<LogicalType> struct_children = {
	    {"encoding", LogicalType::VARCHAR}, {"run_ends", LogicalType::VARCHAR}, {"values", LogicalType::VARCHAR}};

	auto h5_ree = ScalarFunction("h5_ree", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                             LogicalType::STRUCT(struct_children), H5ReeFunction);
	CreateScalarFunctionInfo info(std::move(h5_ree));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	info.descriptions.push_back(
	    H5FunctionDescription({LogicalType::VARCHAR, LogicalType::VARCHAR}, {"run_ends_path", "values_path"},
	                          "Creates a run-end encoded column definition for h5_read().",
	                          {"FROM h5_read('data.h5', '/time', h5_ree('/state_run_ends', '/state_values'))"}));
	loader.RegisterFunction(std::move(info));
}

// ==================== h5_alias Scalar Function ====================

static void H5AliasFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vec = args.data[0];
	auto &definition_vec = args.data[1];

	auto &children = StructVector::GetEntries(result);
	D_ASSERT(children.size() == 3);
	auto &tag_child = GetStructChild(children[0]);
	auto &name_child = GetStructChild(children[1]);
	auto &definition_child = GetStructChild(children[2]);
	definition_child.Reference(definition_vec);

	UnifiedVectorFormat name_data;
	name_vec.ToUnifiedFormat(args.size(), name_data);
	auto name_ptr = UnifiedVectorFormat::GetData<string_t>(name_data);

	for (idx_t i = 0; i < args.size(); i++) {
		auto name_idx = name_data.sel->get_index(i);
		FlatVector::GetData<string_t>(tag_child)[i] = StringVector::AddString(tag_child, "__alias__");
		FlatVector::GetData<string_t>(name_child)[i] = StringVector::AddString(name_child, name_ptr[name_idx]);
	}

	bool all_const = definition_vec.GetVectorType() == VectorType::CONSTANT_VECTOR &&
	                 name_vec.GetVectorType() == VectorType::CONSTANT_VECTOR;
	result.SetVectorType(all_const ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
	result.Verify(args.size());
}

static unique_ptr<FunctionData> H5AliasBind(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 2) {
		throw InvalidInputException("h5_alias() requires two arguments: column name and column definition");
	}
	child_list_t<LogicalType> struct_children = {{"tag", LogicalType::VARCHAR},
	                                             {"column_name", arguments[0]->return_type},
	                                             {"definition", arguments[1]->return_type}};
	bound_function.return_type = LogicalType::STRUCT(struct_children);
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

void RegisterH5AliasFunction(ExtensionLoader &loader) {
	ScalarFunction h5_alias("h5_alias", {LogicalType::VARCHAR, LogicalType::ANY}, LogicalTypeId::STRUCT,
	                        H5AliasFunction, H5AliasBind);
	h5_alias.serialize = VariableReturnBindData::Serialize;
	h5_alias.deserialize = VariableReturnBindData::Deserialize;
	CreateScalarFunctionInfo info(std::move(h5_alias));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	info.descriptions.push_back(H5FunctionDescription(
	    {LogicalType::VARCHAR, LogicalType::ANY}, {"name", "definition"}, "Renames a column definition.",
	    {"FROM h5_read('data.h5', h5_alias('temperature', '/entry/temp'))"}));
	loader.RegisterFunction(std::move(info));
}

// ==================== h5_index Scalar Function ====================

static void H5IndexFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &children = StructVector::GetEntries(result);
	D_ASSERT(children.size() == 1);
	auto &tag_child = GetStructChild(children[0]);

	for (idx_t i = 0; i < args.size(); i++) {
		FlatVector::GetData<string_t>(tag_child)[i] = StringVector::AddString(tag_child, "__index__");
	}

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	result.Verify(args.size());
}

void RegisterH5IndexFunction(ExtensionLoader &loader) {
	child_list_t<LogicalType> struct_children = {{"tag", LogicalType::VARCHAR}};
	auto h5_index = ScalarFunction("h5_index", {}, LogicalType::STRUCT(struct_children), H5IndexFunction);
	CreateScalarFunctionInfo info(std::move(h5_index));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	info.descriptions.push_back(H5FunctionDescription({}, {},
	                                                  "Creates a virtual row-index column definition for h5_read().",
	                                                  {"FROM h5_read('data.h5', h5_index(), '/measurements')"}));
	loader.RegisterFunction(std::move(info));
}

// Cardinality function - informs DuckDB's optimizer of exact row count
static unique_ptr<NodeStatistics> H5ReadCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<H5ReadBindData>();
	return make_uniq<NodeStatistics>(bind_data.total_num_rows);
}

static virtual_column_map_t H5ReadGetVirtualColumns(ClientContext &, optional_ptr<FunctionData> bind_data_p) {
	virtual_column_map_t result;
	if (!bind_data_p || !bind_data_p->Cast<H5ReadBindData>().visible_filename_idx.has_value()) {
		result.insert(
		    make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILENAME, TableColumn("filename", LogicalType::VARCHAR)));
	}
	result.insert(make_pair(COLUMN_IDENTIFIER_EMPTY, TableColumn("", LogicalType::BOOLEAN)));
	return result;
}

void RegisterH5ReadTableFunction(ExtensionLoader &loader) {
	// First argument is filename (VARCHAR), then 1+ dataset paths (VARCHAR or STRUCT for encoded columns)
	TableFunction h5_read_function("h5_read", {LogicalType::VARCHAR, LogicalType::ANY}, H5ReadScan, H5ReadBind,
	                               H5ReadInit);
	// Allow additional ANY arguments for multiple datasets (VARCHAR or STRUCT from h5_rse()/h5_ree())
	h5_read_function.varargs = LogicalType::ANY;
	h5_read_function.named_parameters["filename"] = LogicalType::ANY;
	h5_read_function.named_parameters["swmr"] = LogicalType::BOOLEAN;

	// Predicate pushdown: claim filters in bind, build row ranges in init,
	// and scan only matching run-encoded/index ranges while keeping DuckDB's post-scan verification.

	// Enable projection pushdown - only read columns that are actually needed
	h5_read_function.projection_pushdown = true;

	// Enable predicate pushdown for run-encoded and index columns.
	// We claim filters for I/O optimization but keep them for post-scan verification.
	// DuckDB applies all filters post-scan (defensive, ensures correctness)
	h5_read_function.pushdown_complex_filter = H5ReadPushdownComplexFilter;

	// Set cardinality function for query optimizer
	h5_read_function.cardinality = H5ReadCardinality;
	h5_read_function.init_local = H5ReadInitLocal;
	// Do not register get_partition_data for now. The previous implementation
	// derived batch indexes from logical partitions owned by a local scan state
	// across Scan() calls. That made early-stopping plans able to abandon a
	// partition while still blocking shared cache progress for later ranges.
	// Reintroduce this only with a design that does not make cache progress
	// depend on a thread returning to Scan() after it has produced a chunk.
	h5_read_function.get_virtual_columns = H5ReadGetVirtualColumns;
	auto h5_read_set = MultiFileReader::CreateFunctionSet(std::move(h5_read_function));
	CreateTableFunctionInfo info(std::move(h5_read_set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	info.descriptions.push_back(H5FunctionDescription(
	    {LogicalType::ANY, LogicalType::ANY}, {"filename_or_filenames", "dataset_or_definition", "swmr", "filename"},
	    "Reads one or more HDF5 datasets as DuckDB columns.", {"FROM h5_read('data.h5', '/measurements')"}));
	loader.RegisterFunction(std::move(info));
}

} // namespace duckdb
