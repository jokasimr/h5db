#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "h5_raii.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
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
#include <exception>
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

static constexpr idx_t H5_READ_LOGICAL_PARTITION_MULTIPLIER = 10;
static constexpr idx_t H5_READ_LOGICAL_PARTITION_SIZE =
    H5_READ_LOGICAL_PARTITION_MULTIPLIER * STANDARD_VECTOR_SIZE;
static constexpr idx_t H5_READ_WIDE_ROW_FEW_ROWS_THRESHOLD = 64 * 1024;

static string FormatRemoteHDF5Error(const string &prefix, const string &filename) {
	return FormatRemoteFileError(prefix, filename);
}

static string FormatRemoteDatasetReadError(const string &filename, const string &dataset_path) {
	return AppendRemoteError("Failed to read data from dataset: " + dataset_path, filename);
}

static string FormatInvalidDatasetStringError(const string &filename, const string &dataset_path) {
	return AppendRemoteError("Invalid unicode (byte sequence mismatch) detected in dataset: " + dataset_path, filename);
}

static string ValidateHDF5StringValue(string value, H5T_cset_t cset, const string &filename,
                                      const string &dataset_path) {
	if (!H5StringMatchesCharset(value, cset)) {
		throw IOException(FormatInvalidDatasetStringError(filename, dataset_path));
	}
	return value;
}

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
	size_t element_size;     // Bytes per row (1D: size of element, ND: product of inner dims)
	size_t elements_per_row; // Element count per row (1D: 1, ND: product of inner dims)
};

// Scalar dataset specification (rank-0)
struct ScalarColumnSpec {
	std::string path;
	std::string column_name;
	LogicalType column_type;
	bool is_string;
	std::optional<H5TypeHandle> string_h5_type; // Present only for string datasets
};

// Run-Start Encoded column specification
struct RSEColumnSpec {
	std::string run_starts_path;
	std::string values_path;
	std::string column_name;
	LogicalType column_type;
	std::optional<H5TypeHandle> values_string_h5_type; // Present only for string values datasets
};

// Virtual index column specification
struct IndexColumnSpec {
	std::string column_name;
	LogicalType column_type;
};

// A column can be regular, RSE, or virtual index
using ColumnSpec = std::variant<RegularColumnSpec, ScalarColumnSpec, RSEColumnSpec, IndexColumnSpec>;

// Chunk cache data (separate struct to allow unique_ptr due to non-movable atomics)
struct Chunk {
	idx_t chunk_size = 0; // Rows per chunk

	// Typed storage - buffer with chunk_size * elements_per_row capacity
	using CacheStorage =
	    std::variant<std::monostate, // No cache (for non-cacheable columns)
	                 std::vector<int8_t>, std::vector<int16_t>, std::vector<int32_t>, std::vector<int64_t>,
	                 std::vector<uint8_t>, std::vector<uint16_t>, std::vector<uint32_t>, std::vector<uint64_t>,
	                 std::vector<float>, std::vector<double>>;
	CacheStorage cache; // Size: chunk_size * elements_per_row elements

	// Chunk state tracking
	// end_row is one past the logical cache window start plus chunk_size.
	// This makes the cache window start recoverable as (end_row - chunk_size).
	// Initialized to 0, which makes chunk appear stale (covers negative range).
	std::atomic<idx_t> end_row {0};
};

struct ChunkCache {
	static constexpr idx_t MAX_CHUNKS = 2;

	Chunk chunks[MAX_CHUNKS]; // Fixed size array (atomic members prevent std::vector usage)
};

// Regular column runtime state
struct RegularColumnState {
	H5DatasetHandle dataset;      // RAII wrapper - automatic cleanup
	H5DataspaceHandle file_space; // Cached dataspace handle (reused across chunks)

	// Chunk caching (unique_ptr because mutex/cv are non-movable)
	std::unique_ptr<ChunkCache> chunk_cache;
};

// Scalar column runtime state (cached value)
struct ScalarColumnState {
	using ScalarValue =
	    std::variant<int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t, float, double, string>;
	ScalarValue value;
};

// RSE column runtime state
struct RSEColumnState {
	std::vector<idx_t> run_starts;

	// Typed storage for values (eliminates Value object overhead)
	using RSEValueStorage =
	    std::variant<std::vector<int8_t>, std::vector<int16_t>, std::vector<int32_t>, std::vector<int64_t>,
	                 std::vector<uint8_t>, std::vector<uint16_t>, std::vector<uint32_t>, std::vector<uint64_t>,
	                 std::vector<float>, std::vector<double>, std::vector<string>>;
	RSEValueStorage values;

	// Note: No mutable state needed - ScanRSEColumn is now stateless (thread-safe)
};

struct IndexColumnState {};

// Runtime state for a column (regular, scalar, RSE, or index)
using ColumnState = std::variant<RegularColumnState, ScalarColumnState, RSEColumnState, IndexColumnState>;

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
	bool had_glob = false;

	bool SupportStatementCache() const override {
		return !had_glob;
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
	// position is the next globally unclaimed logical partition start.
	std::atomic<idx_t> position {0};
	std::atomic<idx_t> position_done {0}; // All rows in [0, position_done) have been returned or filtered out

	// Row range filtering (for predicate pushdown on RSE or index columns)
	vector<RowRange> valid_row_ranges; // Sorted, non-overlapping ranges to scan
	idx_t scan_batch_size = STANDARD_VECTOR_SIZE;

	// Mutex for thread-safe range selection (enables parallel scanning)
	std::mutex range_selection_mutex;

	// Track out-of-order scan completions for position_done advancement
	// Maps scan start position -> scan end position for completed scans
	// that couldn't be merged into position_done yet (due to gaps)
	std::map<idx_t, idx_t> completed_ranges;

	// Chunk loading coordination: only one thread loads chunks at a time
	// Other threads proceed with scanning cached data (enables parallel processing)
	std::atomic<bool> someone_is_fetching {false};
	// Fallback for platforms without std::atomic::wait/notify.
	std::mutex fetch_mutex;
	std::condition_variable fetch_cv;

	// No destructor needed - RAII wrappers handle all cleanup automatically
};

struct H5ReadLocalState {
	idx_t position = 0;
	idx_t position_end = 0;
};

struct H5ReadMultiFileGlobalState : public GlobalTableFunctionState {
	vector<column_t> data_column_ids;
	vector<idx_t> data_output_column_positions;
	vector<idx_t> filename_output_positions;
	unique_ptr<H5ReadGlobalState> active_global_state;
	idx_t active_file_idx = 0;
	idx_t max_threads = GlobalTableFunctionState::MAX_THREADS;
	vector<idx_t> partition_bases;
	idx_t active_participants = 0;
	std::exception_ptr pending_exception;
	std::mutex transition_lock;
	std::condition_variable transition_cv;

	idx_t MaxThreads() const override {
		return max_threads;
	}
};

struct H5ReadMultiFileLocalState : public LocalTableFunctionState {
	unique_ptr<H5ReadLocalState> active_local_state;
	idx_t partition_base = 0;
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

// Helper: get base (non-array) type from a possibly nested array type
static LogicalType GetBaseType(LogicalType type) {
	while (type.id() == LogicalTypeId::ARRAY) {
		type = ArrayType::GetChildType(type);
	}
	return type;
}

// Helper: get innermost vector and base type for array columns
static Vector &GetInnermostVector(Vector &vector, const LogicalType &type, LogicalType &base_type) {
	Vector *current_vector = &vector;
	LogicalType current_type = type;
	while (current_type.id() == LogicalTypeId::ARRAY) {
		current_vector = &ArrayVector::GetEntry(*current_vector);
		current_type = ArrayType::GetChildType(current_type);
	}
	base_type = current_type;
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
                                        vector<idx_t> &filename_output_positions) {
	data_column_ids.clear();
	data_output_positions.clear();
	filename_output_positions.clear();

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
	for (idx_t output_idx = 0; output_idx < column_ids.size(); output_idx++) {
		auto column_id = column_ids[output_idx];
		if (H5ReadIsDataColumn(bind_data, column_id)) {
			data_column_ids.push_back(column_id);
			data_output_positions.push_back(output_idx);
		} else if (H5ReadIsFilenameColumn(bind_data, column_id)) {
			filename_output_positions.push_back(output_idx);
		}
	}
}

// Helper function to build nested array types for multi-dimensional datasets
static LogicalType BuildArrayType(LogicalType base_type, const std::vector<hsize_t> &dims, int ndims) {
	if (ndims == 0) {
		return base_type;
	}
	if (ndims == 1) {
		return base_type;
	}

	if (ndims > 4) {
		throw IOException("Datasets with more than 4 dimensions are not currently supported");
	}

	// Build nested array types from innermost to outermost
	LogicalType result = base_type;
	for (int i = ndims - 1; i >= 1; i--) {
		result = LogicalType::ARRAY(result, dims[i]);
	}
	return result;
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

static idx_t ComputeChunkSize(const RegularColumnSpec &spec, hid_t dataset_id, idx_t target_batch_size_bytes,
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

	idx_t chunk_size;
	if (chunk_rows > 0) {
		auto row_bytes = MaxValue<idx_t>(spec.element_size, 1);
		idx_t target_rows = MaxValue<idx_t>(target_batch_size_bytes / row_bytes, 1);
		target_rows = MaxValue<idx_t>(target_rows, chunk_rows);
		idx_t remainder = target_rows % chunk_rows;
		if (remainder != 0) {
			target_rows += (chunk_rows - remainder);
		}
		chunk_size = target_rows;
		D_ASSERT(chunk_size % chunk_rows == 0);
	} else {
		chunk_size = MaxValue<idx_t>(target_batch_size_bytes / MaxValue<idx_t>(spec.element_size, 1), 1);
	}
	if (total_rows == 0) {
		return 0;
	}
	return MinValue<idx_t>(chunk_size, total_rows);
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
		hsize_t chunk_size = 0;
		if (H5Dget_chunk_info_by_coord(dataset_id, chunk_origin.data(), &filter_mask, &chunk_address, &chunk_size) >=
		        0 &&
		    chunk_address != HADDR_UNDEF && chunk_size > 0) {
			result = chunk_address;
		}
	}

	H5Pclose(dcpl);
	return result;
}

// Helper function to open a dataset and get its type in one operation
static std::pair<H5DatasetHandle, H5TypeHandle> OpenDatasetAndGetType(hid_t file, const std::string &filename,
                                                                      const std::string &path) {
	// Open dataset (with error suppression)
	H5DatasetHandle dataset;
	{
		H5ErrorSuppressor suppress;
		dataset = H5DatasetHandle(file, path.c_str());
	}

	if (!dataset.is_valid()) {
		throw IOException(AppendRemoteError("Failed to open dataset: " + path, filename));
	}

	// Get datatype
	hid_t type_id = H5Dget_type(dataset);
	if (type_id < 0) {
		throw IOException(AppendRemoteError("Failed to get dataset type: " + path, filename));
	}

	return {std::move(dataset), H5TypeHandle::TakeOwnershipOf(type_id)};
}

// Helper function to read HDF5 strings (handles both variable-length and fixed-length)
// The callback is called for each string: callback(index, string_value)
static void ReadHDF5Strings(hid_t dataset_id, hid_t h5_type, hid_t mem_space, hid_t file_space, idx_t count,
                            const string &filename, const string &dataset_path,
                            std::function<void(idx_t, const std::string &)> callback) {
	if (count == 0) {
		return;
	}
	htri_t is_variable = H5Tis_variable_str(h5_type);
	if (is_variable < 0) {
		throw IOException(AppendRemoteError("Failed to inspect string type for dataset: " + dataset_path, filename));
	}
	H5T_cset_t cset = H5Tget_cset(h5_type);
	if (cset == H5T_CSET_ERROR) {
		throw IOException(AppendRemoteError("Failed to inspect string charset for dataset: " + dataset_path, filename));
	}

	if (is_variable > 0) {
		// Variable-length strings
		std::vector<char *> string_data(count);

		H5ErrorSuppressor suppress;
		herr_t status = H5Dread(dataset_id, h5_type, mem_space, file_space, H5P_DEFAULT, string_data.data());

		if (status < 0) {
			throw IOException(FormatRemoteDatasetReadError(filename, dataset_path));
		}

		hsize_t mem_dim = count;
		H5DataspaceHandle reclaim_space(1, &mem_dim);
		auto reclaim = [&]() {
			if (H5Dvlen_reclaim(h5_type, reclaim_space, H5P_DEFAULT, string_data.data()) < 0) {
				throw IOException(AppendRemoteError(
				    "Failed to reclaim variable-length string data from dataset: " + dataset_path, filename));
			}
		};

		try {
			// Process strings via callback
			for (idx_t i = 0; i < count; i++) {
				if (string_data[i]) {
					callback(i, ValidateHDF5StringValue(string(string_data[i]), cset, filename, dataset_path));
				} else {
					// Treat NULL strings as empty strings for consistency with h5py.
					callback(i, string());
				}
			}
		} catch (...) {
			try {
				reclaim();
			} catch (...) {
			}
			throw;
		}
		reclaim();

	} else {
		// Fixed-length strings
		size_t str_len = H5Tget_size(h5_type);
		H5T_str_t strpad = H5Tget_strpad(h5_type);
		if (strpad == H5T_STR_ERROR) {
			throw IOException(
			    AppendRemoteError("Failed to inspect string padding for dataset: " + dataset_path, filename));
		}
		std::vector<char> buffer(count * str_len);

		H5ErrorSuppressor suppress;
		herr_t status = H5Dread(dataset_id, h5_type, mem_space, file_space, H5P_DEFAULT, buffer.data());

		if (status < 0) {
			throw IOException(FormatRemoteDatasetReadError(filename, dataset_path));
		}

		// Process strings via callback
		for (idx_t i = 0; i < count; i++) {
			auto *str_ptr = buffer.data() + (i * str_len);
			auto decoded = H5DecodeFixedLengthString(str_ptr, str_len, strpad);
			callback(i, ValidateHDF5StringValue(std::move(decoded), cset, filename, dataset_path));
		}
	}
}

//===--------------------------------------------------------------------===//
// Predicate Pushdown Helpers (for RSE columns)
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

static bool EvaluateValueComparison(const Value &value, ExpressionType comparison, const Value &filter_val,
                                    const LogicalType &comparison_type);
static vector<RowRange> IntersectRowRanges(const vector<RowRange> &a, const vector<RowRange> &b);

static vector<RowRange> BuildIndexRanges(const vector<ClaimedFilter> &filters, idx_t num_rows) {
	auto evaluate_index_filter = [](idx_t index, const ClaimedFilter &filter) {
		return EvaluateValueComparison(Value::BIGINT(static_cast<int64_t>(index)), filter.comparison, filter.constant,
		                               filter.comparison_type);
	};

	auto build_single_filter_ranges = [&](const ClaimedFilter &filter) -> vector<RowRange> {
		auto find_first_true = [&](ExpressionType comparison) -> idx_t {
			idx_t lo = 0;
			idx_t hi = num_rows;
			ClaimedFilter temp_filter = filter;
			temp_filter.comparison = comparison;
			while (lo < hi) {
				idx_t mid = lo + (hi - lo) / 2;
				if (evaluate_index_filter(mid, temp_filter)) {
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
			ClaimedFilter temp_filter = filter;
			temp_filter.comparison = comparison;
			while (lo < hi) {
				idx_t mid = lo + (hi - lo) / 2;
				if (evaluate_index_filter(mid, temp_filter)) {
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

static bool EvaluateValueComparison(const Value &value, ExpressionType comparison, const Value &filter_val,
                                    const LogicalType &comparison_type) {
	Value lhs = value;
	Value rhs = filter_val;
	if (comparison_type.IsValid()) {
		if (!lhs.DefaultTryCastAs(comparison_type, true) || !rhs.DefaultTryCastAs(comparison_type, true)) {
			return false;
		}
	}
	switch (comparison) {
	case ExpressionType::COMPARE_EQUAL:
		return ValueOperations::Equals(lhs, rhs);
	case ExpressionType::COMPARE_GREATERTHAN:
		return ValueOperations::GreaterThan(lhs, rhs);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ValueOperations::GreaterThanEquals(lhs, rhs);
	case ExpressionType::COMPARE_LESSTHAN:
		return ValueOperations::LessThan(lhs, rhs);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return ValueOperations::LessThanEquals(lhs, rhs);
	default:
		return false;
	}
}

static vector<RowRange> BuildRangesForRSEColumn(const RSEColumnSpec &rse_spec, const RSEColumnState &rse_state,
                                                const vector<ClaimedFilter> &col_filters, hsize_t num_rows) {
	return DispatchOnDuckDBType(rse_spec.column_type, [&](auto type_tag) -> vector<RowRange> {
		using T = typename decltype(type_tag)::type;
		auto &typed_values = std::get<std::vector<T>>(rse_state.values);

		// Loop through runs, building ranges where ALL filters are satisfied
		vector<RowRange> col_result;
		idx_t current_start = 0;
		bool in_range = false;

		// Leading NULL segment (if any) never satisfies comparison filters.
		if (!rse_state.run_starts.empty()) {
			current_start = rse_state.run_starts[0];
		}

		for (size_t i = 0; i < typed_values.size(); i++) {
			const T &value = typed_values[i];
			Value run_value = Value::CreateValue(value);
			idx_t run_start = rse_state.run_starts[i];

			// Check if this run's value satisfies ALL filters
			bool satisfies_all = true;
			for (const auto &filter : col_filters) {
				if (!EvaluateValueComparison(run_value, filter.comparison, filter.constant, filter.comparison_type)) {
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
				col_result.push_back({current_start, run_start});
				in_range = false;
			}
		}

		// Close final range if still open
		if (in_range) {
			col_result.push_back({current_start, num_rows});
		}

		return col_result;
	});
}

//===--------------------------------------------------------------------===//
// RSE Helpers
//===--------------------------------------------------------------------===//

static vector<idx_t> LoadRunStarts(const string &filename, const RSEColumnSpec &spec, hid_t starts_ds, size_t num_runs,
                                   hsize_t num_rows) {
	vector<idx_t> run_starts(num_runs);
	H5ErrorSuppressor suppress;
	herr_t status = H5Dread(starts_ds, H5T_NATIVE_UINT64, H5S_ALL, H5S_ALL, H5P_DEFAULT, run_starts.data());
	if (status < 0) {
		throw IOException(AppendRemoteError("Failed to read run_starts from: " + spec.run_starts_path, filename));
	}

	for (size_t i = 1; i < num_runs; i++) {
		if (run_starts[i] <= run_starts[i - 1]) {
			throw IOException("RSE run_starts must be strictly increasing");
		}
	}
	if (num_runs > 0 && run_starts.back() >= num_rows) {
		throw IOException("RSE run_starts contains index " + std::to_string(run_starts.back()) +
		                  " which exceeds dataset length " + std::to_string(num_rows));
	}

	return run_starts;
}

static RSEColumnState::RSEValueStorage LoadRSEValues(const string &filename, const RSEColumnSpec &spec, hid_t values_ds,
                                                     size_t num_values) {
	return DispatchOnDuckDBType(spec.column_type, [&](auto type_tag) -> RSEColumnState::RSEValueStorage {
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
				throw IOException(AppendRemoteError("Failed to read values from: " + spec.values_path, filename));
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
	bool has_rse_columns = false;

	// Process each column (regular dataset, RSE, or index)
	for (size_t i = 0; i < num_columns; i++) {
		const auto &input_val = inputs[i + 1];
		std::optional<std::string> alias_name;
		Value column_val = UnwrapAliasSpec(input_val, alias_name);

		// Check for virtual index or RSE column (STRUCT type)
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

			// RSE column - extract struct fields
			if (children.size() != 3) {
				throw InvalidInputException("Expected h5_rse() to return a struct with 3 fields");
			}

			string encoding = children[0].GetValue<string>();
			string run_starts = children[1].GetValue<string>();
			string values = children[2].GetValue<string>();

			if (encoding != "rse") {
				throw InvalidInputException("Unknown encoding: " + encoding);
			}

			RSEColumnSpec rse_spec;
			rse_spec.run_starts_path = run_starts;
			rse_spec.values_path = values;
			rse_spec.column_name = alias_name ? *alias_name : GetColumnName(values);
			has_rse_columns = true;

			// Open run_starts dataset and get type
			auto [starts_ds, starts_type] = OpenDatasetAndGetType(file, result.filename, run_starts);
			if (H5Tget_class(starts_type) != H5T_INTEGER) {
				throw IOException("RSE run_starts must be integer type");
			}

			// Open values dataset and get type
			auto [values_ds, values_type] = OpenDatasetAndGetType(file, result.filename, values);
			// Determine DuckDB column type from values (before move)
			rse_spec.column_type = H5TypeToDuckDBType(values_type);
			if (H5Tget_class(values_type) == H5T_STRING) {
				rse_spec.values_string_h5_type = std::move(values_type);
			}

			result.columns.push_back(std::move(rse_spec));

		} else {
			// Regular column (may be scalar)
			if (column_val.type().id() != LogicalTypeId::VARCHAR) {
				throw InvalidInputException("h5_read dataset path arguments must be VARCHAR, h5_rse(), h5_index(), or "
				                            "h5_alias(...)");
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
				throw IOException(AppendRemoteError("Failed to get dataset dataspace: " + ds_info.path, result.filename));
			}

			ds_info.ndims = H5Sget_simple_extent_ndims(space);
			if (ds_info.ndims < 0) {
				throw IOException(AppendRemoteError("Failed to get dataset dimensions: " + ds_info.path, result.filename));
			}
			if (ds_info.is_string && ds_info.ndims > 1) {
				throw IOException("String datasets with more than 1 dimension are not supported");
			}

			if (ds_info.ndims == 0) {
				// Scalar dataset - create ScalarColumnSpec and skip regular path
				ScalarColumnSpec scalar_info;
				scalar_info.path = ds_info.path;
				scalar_info.column_name = ds_info.column_name;
				scalar_info.is_string = ds_info.is_string;

				LogicalType base_type = H5TypeToDuckDBType(type);
				scalar_info.column_type = base_type;
				if (scalar_info.is_string) {
					scalar_info.string_h5_type = std::move(type);
				}

				result.columns.push_back(std::move(scalar_info));
				continue;
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

			// Calculate element size for multi-dimensional arrays (before move)
			ds_info.element_size = H5Tget_size(type);
			ds_info.elements_per_row = 1;
			for (int j = 1; j < ds_info.ndims; j++) {
				ds_info.element_size *= ds_info.dims[j];
				ds_info.elements_per_row *= ds_info.dims[j];
			}

			if (ds_info.is_string) {
				// Preserve file-local string metadata for runtime string decoding.
				ds_info.string_h5_type = std::move(type);
			}

			// Build array type for multi-dimensional datasets
			ds_info.column_type = BuildArrayType(base_type, ds_info.dims, ds_info.ndims);

			result.columns.push_back(std::move(ds_info));
		}
	}

	// Require at least one regular column (scalar or non-scalar)
	if (num_regular_columns == 0) {
		throw IOException("h5_read requires at least one regular column");
	}

	// RSE requires a non-scalar regular column for row count
	if (has_rse_columns && non_scalar_regular_columns == 0) {
		throw IOException(
		    "h5_read requires at least one non-scalar regular column when using RSE to determine row count");
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
	result->had_glob = expanded.had_glob;
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
	if (std::holds_alternative<RSEColumnSpec>(bind_data.columns[global_idx])) {
		LocalColumnIdx local_idx = GlobalToLocal(gstate, global_idx);
		auto &rse_spec = std::get<RSEColumnSpec>(bind_data.columns[global_idx]);
		auto &rse_state = std::get<RSEColumnState>(gstate.column_states[local_idx]);
		return BuildRangesForRSEColumn(rse_spec, rse_state, col_filters, bind_data.num_rows);
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
	idx_t projected_numeric_row_bytes = 0;

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
					    throw IOException(
					        AppendRemoteError("Failed to open dataset: " + spec.path, bind_data.filename));
				    }

				    // Cache the file dataspace (reused across all chunks)
				    H5DataspaceHandle file_space(dataset);
				    if (!file_space.is_valid()) {
					    throw IOException(
					        AppendRemoteError("Failed to get dataspace for dataset: " + spec.path, bind_data.filename));
				    }

				    RegularColumnState state;
				    state.dataset = std::move(dataset);
				    state.file_space = std::move(file_space);

				    // Cache numeric columns (strings are read directly)
				    bool is_cacheable = !spec.is_string;
				    if (is_cacheable) {
					    projected_numeric_row_bytes += NumericCast<idx_t>(spec.element_size);
					    idx_t chunk_size =
					        ComputeChunkSize(spec, state.dataset.get(), target_batch_size_bytes, bind_data.num_rows);
					    if (chunk_size > 0) {
						    state.chunk_cache = std::make_unique<ChunkCache>();

						    auto chunk_count = (chunk_size >= bind_data.num_rows) ? 1 : ChunkCache::MAX_CHUNKS;

						    auto base_type = GetBaseType(spec.column_type);
						    DispatchOnNumericType(base_type, [&](auto type_tag) {
							    using T = typename decltype(type_tag)::type;
							    for (idx_t chunk_idx = 0; chunk_idx < chunk_count; chunk_idx++) {
								    auto &chunk = state.chunk_cache->chunks[chunk_idx];
								    idx_t buffer_elements = chunk_size * spec.elements_per_row;
								    chunk.cache = std::vector<T>(buffer_elements);
								    chunk.chunk_size = chunk_size;
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
				    // Scalar column - open dataset and cache value once
				    H5DatasetHandle dataset;
				    {
					    H5ErrorSuppressor suppress;
					    dataset = H5DatasetHandle(result->file, spec.path.c_str());
				    }

				    if (!dataset.is_valid()) {
					    throw IOException(
					        AppendRemoteError("Failed to open dataset: " + spec.path, bind_data.filename));
				    }

				    ScalarColumnState scalar_state;
				    if (spec.is_string) {
					    D_ASSERT(spec.string_h5_type.has_value());
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

			    } else if constexpr (std::is_same_v<T, RSEColumnSpec>) {
				    // RSE column - load run_starts and values using stored types from Bind
				    RSEColumnState rse_col;

				    // Open datasets (types were inspected in Bind phase) - RAII handles cleanup
				    H5DatasetHandle starts_ds;
				    H5DatasetHandle values_ds;
				    {
					    H5ErrorSuppressor suppress;
					    starts_ds = H5DatasetHandle(result->file, spec.run_starts_path.c_str());
					    if (!starts_ds.is_valid()) {
						    throw IOException(AppendRemoteError(
						        "Failed to open RSE run_starts dataset: " + spec.run_starts_path, bind_data.filename));
					    }

					    values_ds = H5DatasetHandle(result->file, spec.values_path.c_str());
					    if (!values_ds.is_valid()) {
						    throw IOException(AppendRemoteError(
						        "Failed to open RSE values dataset: " + spec.values_path, bind_data.filename));
					    }
				    }

				    // Get array sizes
				    H5DataspaceHandle starts_space(starts_ds);
				    int starts_ndims = H5Sget_simple_extent_ndims(starts_space);
				    if (starts_ndims < 0) {
					    throw IOException("Failed to get dimensions for RSE run_starts dataset: " +
					                      spec.run_starts_path);
				    }
				    if (starts_ndims != 1) {
					    throw IOException("RSE run_starts must be a 1-dimensional dataset");
				    }
				    hssize_t num_runs_hssize = H5Sget_simple_extent_npoints(starts_space);

				    H5DataspaceHandle values_space(values_ds);
				    int values_ndims = H5Sget_simple_extent_ndims(values_space);
				    if (values_ndims < 0) {
					    throw IOException("Failed to get dimensions for RSE values dataset: " + spec.values_path);
				    }
				    if (values_ndims != 1) {
					    throw IOException("RSE values must be a 1-dimensional dataset");
				    }
				    hssize_t num_values_hssize = H5Sget_simple_extent_npoints(values_space);

				    if (num_runs_hssize < 0 || num_values_hssize < 0) {
					    throw IOException(
					        AppendRemoteError("Failed to get dataset sizes for RSE column", bind_data.filename));
				    }

				    size_t num_runs = static_cast<size_t>(num_runs_hssize);
				    size_t num_values = static_cast<size_t>(num_values_hssize);

				    // Validate: run_starts and values must have same size
				    if (num_runs != num_values) {
					    throw IOException("RSE run_starts and values must have same size. Got " +
					                      std::to_string(num_runs) + " and " + std::to_string(num_values));
				    }

				    rse_col.run_starts =
				        LoadRunStarts(bind_data.filename, spec, starts_ds, num_runs, bind_data.num_rows);
				    rse_col.values = LoadRSEValues(bind_data.filename, spec, values_ds, num_values);

				    // Note: RSEColumnState is now stateless (thread-safe)
				    // No runtime state initialization needed

				    // Store in dense array with LOCAL indexing
				    result->column_states.push_back(std::move(rse_col));
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

	if (projected_numeric_row_bytes > 0) {
		auto target_rows = MaxValue<idx_t>(target_batch_size_bytes / projected_numeric_row_bytes, 1);
		result->scan_batch_size = MinValue<idx_t>(target_rows, STANDARD_VECTOR_SIZE);
	}

	// Compute row ranges based on claimed filters (from pushdown_complex_filter)
	// Group claimed filters by column
	unordered_map<idx_t, vector<ClaimedFilter>> filters_by_column;
	for (const auto &filter : bind_data.claimed_filters) {
		filters_by_column[filter.column_index].push_back(filter);
	}

	// If we have filters on RSE or index columns, compute row ranges
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

	result->position = 0;
	result->position_done = AdjustPositionDoneForRanges(result->valid_row_ranges, 0);

	return result;
}

static idx_t ComputeProjectedNumericRowBytes(const H5ReadBindData &bind_data, const vector<column_t> &data_column_ids) {
	const auto &columns = GetCanonicalColumns(bind_data);
	idx_t projected_numeric_row_bytes = 0;
	for (auto column_id : data_column_ids) {
		if (auto regular_spec = std::get_if<RegularColumnSpec>(&columns[column_id]); regular_spec &&
		    !regular_spec->is_string) {
			projected_numeric_row_bytes += NumericCast<idx_t>(regular_spec->element_size);
		}
	}
	return projected_numeric_row_bytes;
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

static const BoundColumnRefExpression *ExtractPushdownColumnRef(const Expression &expr, LogicalType &comparison_type) {
	comparison_type = expr.return_type;
	const Expression *current = &expr;
	while (current->expression_class == ExpressionClass::BOUND_CAST) {
		auto &cast = current->Cast<BoundCastExpression>();
		if (cast.try_cast) {
			return nullptr;
		}
		current = cast.child.get();
	}
	if (current->expression_class != ExpressionClass::BOUND_COLUMN_REF) {
		return nullptr;
	}
	return &current->Cast<BoundColumnRefExpression>();
}

// Helper: Try to claim a filter on an RSE or index column
template <typename TableIndexT>
static bool TryClaimPushdownFilter(const unique_ptr<Expression> &expr, const TableIndexT &table_index,
                                   const unordered_map<idx_t, idx_t> &get_to_bind_map,
                                   const unordered_set<idx_t> &pushdown_columns, const vector<ColumnSpec> &columns,
                                   vector<ClaimedFilter> &claimed) {
	// Handle comparison expressions: col > 10, col = 20, 10 < col, etc.
	if (expr->expression_class == ExpressionClass::BOUND_COMPARISON) {
		auto &comp = expr->Cast<BoundComparisonExpression>();

		const BoundColumnRefExpression *colref = nullptr;
		const BoundConstantExpression *constant = nullptr;
		LogicalType comparison_type;
		bool need_flip = false;

		// Determine which side is the column and which is the constant
		if (comp.right->expression_class == ExpressionClass::BOUND_CONSTANT) {
			colref = ExtractPushdownColumnRef(*comp.left, comparison_type);
			constant = &comp.right->Cast<BoundConstantExpression>();
			need_flip = false; // col > 10 is already in correct order
		} else if (comp.left->expression_class == ExpressionClass::BOUND_CONSTANT) {
			colref = ExtractPushdownColumnRef(*comp.right, comparison_type);
			constant = &comp.left->Cast<BoundConstantExpression>();
			need_flip = true; // 10 < col needs to become col > 10
		}

		// If we found a column-constant comparison, try to claim it
		if (colref && constant && colref->binding.table_index == table_index) {
			// Map from LogicalGet column index to bind_data column index
			auto it = get_to_bind_map.find(colref->binding.column_index);
			if (it != get_to_bind_map.end()) {
				idx_t bind_data_col_idx = it->second;

				// Check if this is a pushdown-eligible column
				if (pushdown_columns.count(bind_data_col_idx) > 0) {
					const bool is_index = std::holds_alternative<IndexColumnSpec>(columns[bind_data_col_idx]);
					ExpressionType comparison = need_flip ? FlipComparison(comp.type) : comp.type;

					// Whitelist: Only claim comparison operators we can optimize with row ranges
					// This matches the operators handled in the row range computation code
					switch (comparison) {
					case ExpressionType::COMPARE_EQUAL:
					case ExpressionType::COMPARE_GREATERTHAN:
					case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
					case ExpressionType::COMPARE_LESSTHAN:
					case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
						Value constant_value = constant->value;
						if (is_index && constant->value.IsNull()) {
							return false;
						}
						// These we can optimize - claim the filter
						ClaimedFilter filter;
						filter.column_index = bind_data_col_idx;
						filter.comparison = comparison;
						filter.constant = std::move(constant_value);
						filter.comparison_type = comparison_type;
						claimed.push_back(filter);
						return true;
					}
					default:
						// Unknown/unsupported comparison (e.g., !=, IS DISTINCT FROM, etc.)
						// Don't claim - let DuckDB handle it post-scan
						return false;
					}
				}
			}
		}
	}

	// Handle BETWEEN: col BETWEEN lower AND upper
	if (expr->expression_class == ExpressionClass::BOUND_BETWEEN) {
		auto &between = expr->Cast<BoundBetweenExpression>();
		LogicalType comparison_type;
		auto *colref = ExtractPushdownColumnRef(*between.input, comparison_type);

		// Check if input is an RSE column reference
		if (colref && between.lower->expression_class == ExpressionClass::BOUND_CONSTANT &&
		    between.upper->expression_class == ExpressionClass::BOUND_CONSTANT) {
			auto &lower_const = between.lower->Cast<BoundConstantExpression>();
			auto &upper_const = between.upper->Cast<BoundConstantExpression>();

			if (colref->binding.table_index == table_index) {
				// Map from LogicalGet column index to bind_data column index
				auto it = get_to_bind_map.find(colref->binding.column_index);
				if (it != get_to_bind_map.end()) {
					idx_t bind_data_col_idx = it->second;

					// Check if this is a pushdown-eligible column
					if (pushdown_columns.count(bind_data_col_idx) > 0) {
						const bool is_index = std::holds_alternative<IndexColumnSpec>(columns[bind_data_col_idx]);
						Value lower_value = lower_const.value;
						Value upper_value = upper_const.value;
						if (is_index && (lower_const.value.IsNull() || upper_const.value.IsNull())) {
							return false;
						}
						// Claim BETWEEN using its actual inclusive/exclusive bounds
						ClaimedFilter lower_filter;
						lower_filter.column_index = bind_data_col_idx;
						lower_filter.comparison = between.LowerComparisonType();
						lower_filter.constant = std::move(lower_value);
						lower_filter.comparison_type = comparison_type;
						claimed.push_back(lower_filter);

						ClaimedFilter upper_filter;
						upper_filter.column_index = bind_data_col_idx;
						upper_filter.comparison = between.UpperComparisonType();
						upper_filter.constant = std::move(upper_value);
						upper_filter.comparison_type = comparison_type;
						claimed.push_back(upper_filter);

						return true; // Indicate we claimed this filter
					}
				}
			}
		}
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
				                                      temp_claimed);
			}

			// If we claimed any pushdown filters, add them to optimize I/O and return true
			if (claimed_any) {
				claimed.insert(claimed.end(), temp_claimed.begin(), temp_claimed.end());
				return true; // Indicate we claimed something
			}

			// No RSE filters in this conjunction
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

	// Build set of pushdown-eligible column indices (RSE or index)
	unordered_set<idx_t> pushdown_column_indices;
	for (idx_t i = 0; i < columns.size(); i++) {
		if (std::holds_alternative<RSEColumnSpec>(columns[i]) || std::holds_alternative<IndexColumnSpec>(columns[i])) {
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

	// Claim filters for I/O optimization (but keep them in filter list for post-scan)
	// DuckDB will apply all filters after scan to ensure correctness (defensive approach)
	for (const auto &expr : filters) {
		TryClaimPushdownFilter(expr, table_index, get_to_bind_map, pushdown_column_indices, columns,
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

static RangeSelection NextRangeFrom(const std::vector<RowRange> &valid_row_ranges, idx_t position, idx_t batch_size) {
	return NextRangeFrom(valid_row_ranges, position, NumericLimits<idx_t>::Maximum(), batch_size);
}

static RangeSelection NextRangeFrom(const std::vector<RowRange> &valid_row_ranges, idx_t position) {
	return NextRangeFrom(valid_row_ranges, position, NumericLimits<idx_t>::Maximum(), NumericLimits<idx_t>::Maximum());
}

static idx_t GetChunkCount(const ChunkCache &cache, idx_t total_rows) {
	auto chunk_size = cache.chunks[0].chunk_size;
	if (chunk_size == 0 || chunk_size >= total_rows) {
		return 1;
	}
	return ChunkCache::MAX_CHUNKS;
}

static idx_t GetLogicalPartitionStart(idx_t position) {
	return (position / H5_READ_LOGICAL_PARTITION_SIZE) * H5_READ_LOGICAL_PARTITION_SIZE;
}

static bool ClaimNextPartition(H5ReadGlobalState &gstate, H5ReadLocalState &lstate) {
	std::lock_guard<std::mutex> lock(gstate.range_selection_mutex);
	auto position = gstate.position.load();
	auto next_range = NextRangeFrom(gstate.valid_row_ranges, position, 1);
	if (!next_range.has_data) {
		return false;
	}

	// gstate.position advances in logical partition units. lstate.position starts
	// at the first valid row inside the claimed partition, while lstate.position_end
	// remains the exclusive logical partition boundary.
	auto partition_start = GetLogicalPartitionStart(next_range.position);
	D_ASSERT(partition_start >= position);
	lstate.position = next_range.position;
	lstate.position_end = partition_start + H5_READ_LOGICAL_PARTITION_SIZE;
	gstate.position.store(lstate.position_end);
	return true;
}

// Helper function to determine the next data range to read
static RangeSelection GetNextDataRange(H5ReadGlobalState &gstate, H5ReadLocalState &lstate, idx_t num_rows) {
	if (lstate.position == lstate.position_end) {
		if (!ClaimNextPartition(gstate, lstate)) {
			return {false, 0, 0};
		}
	}

	auto partition_end = MinValue<idx_t>(lstate.position_end, num_rows);
	auto range = NextRangeFrom(gstate.valid_row_ranges, lstate.position, partition_end, gstate.scan_batch_size);
	D_ASSERT(range.has_data);

	lstate.position = range.position + range.to_read;
	if (!NextRangeFrom(gstate.valid_row_ranges, lstate.position, partition_end, 1).has_data) {
		lstate.position = lstate.position_end;
	}
	return range;
}

// Helper function to scan an RSE column
static void ScanRSEColumn(const RSEColumnSpec &spec, RSEColumnState &state, Vector &result_vector, idx_t position,
                          idx_t to_read, hsize_t num_rows) {
	// Dispatch once per chunk, not per row
	DispatchOnDuckDBType(spec.column_type, [&](auto type_tag) {
		using T = typename decltype(type_tag)::type;

		// Access typed vector directly (no Value overhead)
		const auto &typed_values = std::get<std::vector<T>>(state.values);
		if (state.run_starts.empty() && typed_values.empty()) {
			// Empty RSE encoding: emit NULLs for the requested rows
			result_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result_vector, true);
			return;
		}

		idx_t result_offset = 0;

		// Leading NULL segment if first run starts after 0
		if (!state.run_starts.empty() && position < state.run_starts[0]) {
			result_offset = std::min<idx_t>(to_read, state.run_starts[0] - position);
			if (result_offset == to_read) {
				result_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(result_vector, true);
				return;
			}
			position += result_offset;
			to_read -= result_offset;
		}

		auto it = std::upper_bound(state.run_starts.begin(), state.run_starts.end(), position);
		idx_t current_run = (it - state.run_starts.begin()) - 1;
		idx_t next_run_start =
		    (current_run + 1 < state.run_starts.size()) ? state.run_starts[current_run + 1] : num_rows;

		// OPTIMIZATION: Check if entire chunk belongs to single run
		// With avg run length ~10k and chunk size 2048, this is true ~83% of the time!
		idx_t rows_in_current_run = next_run_start - position;
		if (result_offset == 0 && rows_in_current_run >= to_read) {
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
				next_run_start =
				    (current_run + 1 < state.run_starts.size()) ? state.run_starts[current_run + 1] : num_rows;
			}
		}
	});
}

// ==================== Chunk Caching Helpers ====================

// Helper: Read data from HDF5 into typed cache buffer
static void ReadIntoTypedCache(Chunk::CacheStorage &cache, hid_t dataset_id, hid_t file_space_id,
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
static void CopyFromTypedCache(const Chunk::CacheStorage &cache, idx_t buffer_offset_rows, idx_t rows_to_copy,
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

static void TryLoadChunks(ChunkCache &cache, hid_t dataset_id, hid_t file_space_id,
                          const std::vector<RowRange> &valid_row_ranges, std::atomic<idx_t> &position_done,
                          idx_t total_rows, const RegularColumnSpec &spec, const string &filename) {
	auto chunk_count = GetChunkCount(cache, total_rows);
	idx_t max_end_row = 0;
	for (idx_t i = 0; i < chunk_count; i++) {
		auto &chunk = cache.chunks[i];
		auto end_row = chunk.end_row.load(std::memory_order_acquire);
		max_end_row = end_row > max_end_row ? end_row : max_end_row;
	}
	auto position_done_value = position_done.load(std::memory_order_acquire);
	for (idx_t i = 0; i < chunk_count; i++) {
		auto &chunk = cache.chunks[i];
		if (chunk.end_row.load(std::memory_order_acquire) <= position_done_value) {
			// This chunk's previous window is fully behind position_done, so every row
			// in that window has already been returned or skipped and the chunk can be
			// reused for the next unread range.
			auto next_range = NextRangeFrom(valid_row_ranges, max_end_row);
			if (next_range.has_data) {
				idx_t rows_to_load = std::min(chunk.chunk_size, total_rows - next_range.position);
				ReadIntoTypedCache(chunk.cache, dataset_id, file_space_id, next_range.position, rows_to_load, spec,
				                   filename);

				idx_t new_end = next_range.position + chunk.chunk_size;
				chunk.end_row.store(new_end, std::memory_order_release);

				max_end_row = new_end;
			}
		}
	}
}

static void NotifyFetchComplete(H5ReadGlobalState &gstate) {
#if defined(__cpp_lib_atomic_wait) && __cpp_lib_atomic_wait >= 201907L
	gstate.someone_is_fetching.notify_all();
#else
	gstate.fetch_cv.notify_all();
#endif
}

static void WaitForFetchComplete(H5ReadGlobalState &gstate) {
#if defined(__cpp_lib_atomic_wait) && __cpp_lib_atomic_wait >= 201907L
	gstate.someone_is_fetching.wait(true, std::memory_order_relaxed);
#else
	std::unique_lock<std::mutex> lock(gstate.fetch_mutex);
	gstate.fetch_cv.wait(lock, [&]() { return !gstate.someone_is_fetching.load(std::memory_order_acquire); });
#endif
}

static void TryRefreshCache(H5ReadGlobalState &gstate, const H5ReadSingleFileBindView &bind_data) {
	bool expected = false;
	if (gstate.someone_is_fetching.compare_exchange_strong(expected, true)) {
		// Exactly one thread refreshes chunk caches at a time. Other threads return
		// immediately here and only block later if the chunks covering their read
		// range are still not available.
		try {
			for (auto local_idx : gstate.cache_refresh_order) {
				GlobalColumnIdx global_idx = GetGlobalIdx(gstate, local_idx);
				const auto &spec = std::get<RegularColumnSpec>(bind_data.columns[global_idx]);
				auto &state = std::get<RegularColumnState>(gstate.column_states[local_idx]);
				D_ASSERT(state.chunk_cache);

				TryLoadChunks(*state.chunk_cache, state.dataset.get(), state.file_space.get(), gstate.valid_row_ranges,
				              gstate.position_done, bind_data.num_rows, spec, bind_data.filename);
			}
		} catch (...) {
			gstate.someone_is_fetching.store(false);
			NotifyFetchComplete(gstate);
			throw;
		}
		// Done loading - release the flag so another thread can load next time
		gstate.someone_is_fetching.store(false);
		NotifyFetchComplete(gstate);
	}
}

// Helper function to scan a regular dataset column
static void ScanRegularColumn(ClientContext &context, const RegularColumnSpec &spec, RegularColumnState &state,
                              Vector &result_vector, idx_t position, idx_t to_read,
                              const H5ReadSingleFileBindView &bind_data, H5ReadGlobalState &gstate) {
	ThrowIfInterrupted(context);
	// Check if using cache
	if (state.chunk_cache) {
		auto &cache = *state.chunk_cache;
		auto chunk_count = GetChunkCount(cache, bind_data.num_rows);
		LogicalType base_type;
		auto &target_vector = GetInnermostVector(result_vector, spec.column_type, base_type);

		auto *chunk1 = &cache.chunks[0];
		Chunk *chunk2 = chunk_count > 1 ? &cache.chunks[1] : nullptr;
		for (;;) {
			ThrowIfInterrupted(context);

			TryRefreshCache(gstate, bind_data);

			idx_t end1 = chunk1->end_row.load(std::memory_order_acquire);
			idx_t end2 = chunk2 ? chunk2->end_row.load(std::memory_order_acquire) : end1;

			if (chunk2 && end1 > end2) {
				std::swap(chunk1, chunk2);
				std::swap(end1, end2);
			}

			if (position + to_read <= end2) {
				break;
			}

			if (gstate.someone_is_fetching.load(std::memory_order_acquire)) {
				WaitForFetchComplete(gstate);
			}
		}

		// Copy data from chunks that overlap our read range [position, position + to_read)
		for (auto *chunk : {chunk1, chunk2}) {
			if (!chunk) {
				continue;
			}
			idx_t chunk_end = chunk->end_row.load(std::memory_order_acquire);
			idx_t chunk_start = (chunk_end > chunk->chunk_size) ? (chunk_end - chunk->chunk_size) : 0;

			// Check if chunk overlaps our range
			if (chunk_start < position + to_read && chunk_end > position) {
				idx_t overlap_start = MaxValue<idx_t>(chunk_start, position);
				idx_t overlap_end = MinValue<idx_t>(chunk_end, position + to_read);
				idx_t overlap_size = overlap_end - overlap_start;

				idx_t chunk_offset = overlap_start - chunk_start;
				idx_t result_offset = overlap_start - position;
				D_ASSERT(chunk_offset + overlap_size <= chunk->chunk_size);

				CopyFromTypedCache(chunk->cache, chunk_offset, overlap_size, target_vector, result_offset, base_type,
				                   spec.elements_per_row);
			}
		}

		return; // Done with cached read
	}

	std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);

	// Non-cached path: fall back to direct HDF5 read
	// Access RAII-wrapped handles from state
	hid_t dataset_id = state.dataset.get();
	hid_t file_space = state.file_space.get();

	// Create memory dataspace for reading
	H5DataspaceHandle mem_space = CreateMemspaceAndSelect(file_space, spec, position, to_read);

	// Read data based on type
	if (spec.is_string) {
		D_ASSERT(spec.string_h5_type.has_value());
		// Handle string data using helper
		ReadHDF5Strings(dataset_id, *spec.string_h5_type, mem_space, file_space, to_read, bind_data.filename,
		                spec.path, [&](idx_t i, const std::string &str) {
			                FlatVector::GetData<string_t>(result_vector)[i] =
			                    StringVector::AddString(result_vector, str);
		                });

	} else {
		// Handle numeric data
		LogicalType base_type;
		auto &target_vector = GetInnermostVector(result_vector, spec.column_type, base_type);
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
static void ScanScalarColumn(const ScalarColumnSpec &spec, const ScalarColumnState &state, Vector &result_vector,
                             idx_t to_read) {
	if (to_read == 0) {
		return;
	}

	if (spec.is_string) {
		const auto &value = std::get<string>(state.value);
		auto result_data = FlatVector::GetData<string_t>(result_vector);
		result_data[0] = StringVector::AddString(result_vector, value);
		result_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		return;
	}

	std::visit(
	    [&](auto &&val) {
		    using T = std::decay_t<decltype(val)>;
		    if constexpr (!std::is_same_v<T, string>) {
			    auto result_data = FlatVector::GetData<T>(result_vector);
			    result_data[0] = val;
			    result_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		    }
	    },
	    state.value);
}

static void H5ReadSingleFileScan(ClientContext &context, const H5ReadSingleFileBindView &bind_data,
                                 H5ReadGlobalState &gstate, H5ReadLocalState &lstate, DataChunk &output) {
	ThrowIfInterrupted(context);

	// Step 2: Determine next data range to read
	auto range_selection = GetNextDataRange(gstate, lstate, bind_data.num_rows);
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

			    if constexpr (std::is_same_v<SpecT, RSEColumnSpec> && std::is_same_v<StateT, RSEColumnState>) {
				    // RSE column - call helper function
				    ScanRSEColumn(spec, state, result_vector, position, to_read, bind_data.num_rows);

			    } else if constexpr (std::is_same_v<SpecT, ScalarColumnSpec> &&
			                         std::is_same_v<StateT, ScalarColumnState>) {
				    // Scalar dataset - broadcast cached value
				    ScanScalarColumn(spec, state, result_vector, to_read);

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

	// Update position_done to track completed scans
	// This scan returned rows [position, position + to_read)
	// position_done uses half-open interval: [0, position_done) has been returned or filtered out
	{
		std::lock_guard<std::mutex> lock(gstate.range_selection_mutex);

		idx_t scan_end = position + to_read;
		idx_t current_done = gstate.position_done.load();

		if (position == current_done) {
			// This scan is contiguous with position_done - advance directly
			gstate.position_done.store(scan_end);

			// Merge any subsequent completed ranges that are now contiguous
			while (true) {
				current_done = gstate.position_done.load();
				current_done = AdjustPositionDoneForRanges(gstate.valid_row_ranges, current_done);
				gstate.position_done.store(current_done);
				auto it = gstate.completed_ranges.find(current_done);
				if (it == gstate.completed_ranges.end()) {
					break;
				}
				gstate.position_done.store(it->second);
				gstate.completed_ranges.erase(it);
			}
			current_done = AdjustPositionDoneForRanges(gstate.valid_row_ranges, gstate.position_done.load());
			gstate.position_done.store(current_done);
		} else {
			// This scan completed out of order - store for later merging
			gstate.completed_ranges[position] = scan_end;
		}
	}
}

static void PublishPendingH5ReadException(H5ReadMultiFileGlobalState &gstate, std::exception_ptr exception) {
	if (!gstate.pending_exception) {
		gstate.pending_exception = exception;
	}
	gstate.transition_cv.notify_all();
}

static void InitializeActiveH5ReadFile(ClientContext &context, const H5ReadBindData &bind_data,
                                       H5ReadMultiFileGlobalState &gstate, idx_t file_idx) {
	gstate.active_global_state = InitSingleH5ReadState(context, GetSingleFileBindView(bind_data, file_idx),
	                                                   gstate.data_column_ids, gstate.data_output_column_positions);
	gstate.active_file_idx = file_idx;
}

static void AttachLocalStateToActiveFile(const H5ReadMultiFileGlobalState &gstate, H5ReadMultiFileLocalState &lstate) {
	lstate.active_local_state = make_uniq<H5ReadLocalState>();
	lstate.partition_base = gstate.partition_bases[gstate.active_file_idx];
}

// Init function - initialize the first file in the multi-file scan wrapper.
static unique_ptr<GlobalTableFunctionState> H5ReadInit(ClientContext &context, TableFunctionInitInput &input) {
	ThrowIfInterrupted(context);
	auto &bind_data = input.bind_data->Cast<H5ReadBindData>();
	auto result = make_uniq<H5ReadMultiFileGlobalState>();
	BuildH5ReadProjectionLayout(bind_data, input.column_ids, result->data_column_ids,
	                            result->data_output_column_positions, result->filename_output_positions);
	auto projected_numeric_row_bytes = ComputeProjectedNumericRowBytes(bind_data, result->data_column_ids);
	if (projected_numeric_row_bytes > 0) {
		idx_t max_num_rows = 0;
		for (const auto &file_bind_data : bind_data.file_bind_data) {
			max_num_rows = MaxValue<idx_t>(max_num_rows, file_bind_data.num_rows);
		}
		if (projected_numeric_row_bytes >= H5_READ_WIDE_ROW_FEW_ROWS_THRESHOLD &&
		    max_num_rows < STANDARD_VECTOR_SIZE) {
			result->max_threads = 1;
		}
	}
	D_ASSERT(!bind_data.file_bind_data.empty());
	InitializeActiveH5ReadFile(context, bind_data, *result, 0);
	result->partition_bases.resize(bind_data.file_bind_data.size());
	idx_t next_partition_base = 0;
	for (idx_t file_idx = 0; file_idx < bind_data.file_bind_data.size(); file_idx++) {
		result->partition_bases[file_idx] = next_partition_base;
		auto file_rows = bind_data.file_bind_data[file_idx].num_rows;
		auto partition_count =
		    MaxValue<idx_t>(1, (file_rows + H5_READ_LOGICAL_PARTITION_SIZE - 1) / H5_READ_LOGICAL_PARTITION_SIZE);
		next_partition_base += partition_count;
	}
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

static void H5ReadScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	ThrowIfInterrupted(context);
	auto &bind_data = data.bind_data->Cast<H5ReadBindData>();
	auto &gstate = data.global_state->Cast<H5ReadMultiFileGlobalState>();
	auto &lstate = data.local_state->Cast<H5ReadMultiFileLocalState>();

	// A local scan state stays attached to the current active file across repeated
	// scan calls. When that local state reaches EOF for the active file, it detaches.
	// The last detaching participant initializes the next file (or marks global EOF)
	// and wakes the waiting threads.
	while (true) {
		idx_t file_idx;
		H5ReadGlobalState *active_gstate;
		{
			std::unique_lock<std::mutex> lock(gstate.transition_lock);
			if (gstate.pending_exception) {
				std::rethrow_exception(gstate.pending_exception);
			}
			if (gstate.active_file_idx >= bind_data.file_bind_data.size()) {
				output.SetCardinality(0);
				return;
			}
			if (!lstate.active_local_state) {
				AttachLocalStateToActiveFile(gstate, lstate);
				gstate.active_participants++;
			}
			file_idx = gstate.active_file_idx;
			active_gstate = gstate.active_global_state.get();
		}

		std::exception_ptr scan_error;
		try {
			H5ReadSingleFileScan(context, GetSingleFileBindView(bind_data, file_idx), *active_gstate,
			                     *lstate.active_local_state, output);
		} catch (...) {
			scan_error = std::current_exception();
		}

		std::unique_lock<std::mutex> lock(gstate.transition_lock);

		if (scan_error) {
			PublishPendingH5ReadException(gstate, scan_error);
		}
		if (gstate.pending_exception) {
			std::rethrow_exception(gstate.pending_exception);
		}

		if (output.size() > 0) {
			H5ReadPopulateFilenameColumns(bind_data, file_idx, gstate, output);
			return;
		}

		D_ASSERT(lstate.active_local_state);
		D_ASSERT(gstate.active_participants > 0);
		lstate.active_local_state.reset();
		gstate.active_participants--;

		if (gstate.active_participants == 0) {
			auto next_file_idx = file_idx + 1;
			gstate.active_global_state.reset();
			if (next_file_idx >= bind_data.file_bind_data.size()) {
				gstate.active_file_idx = next_file_idx;
				gstate.transition_cv.notify_all();
				output.SetCardinality(0);
				return;
			}

			try {
				InitializeActiveH5ReadFile(context, bind_data, gstate, next_file_idx);
				gstate.transition_cv.notify_all();
			} catch (...) {
				PublishPendingH5ReadException(gstate, std::current_exception());
				std::rethrow_exception(gstate.pending_exception);
			}
			continue;
		}

		gstate.transition_cv.wait(lock, [&] { return gstate.active_file_idx != file_idx || gstate.pending_exception; });
		if (gstate.pending_exception) {
			std::rethrow_exception(gstate.pending_exception);
		}
	}
}

// ==================== h5_rse Scalar Function ====================

static void H5RseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &run_starts_vec = args.data[0];
	auto &values_vec = args.data[1];

	UnifiedVectorFormat run_starts_data;
	UnifiedVectorFormat values_data;
	run_starts_vec.ToUnifiedFormat(args.size(), run_starts_data);
	values_vec.ToUnifiedFormat(args.size(), values_data);

	auto run_starts_ptr = UnifiedVectorFormat::GetData<string_t>(run_starts_data);
	auto values_ptr = UnifiedVectorFormat::GetData<string_t>(values_data);

	auto &children = StructVector::GetEntries(result);
	D_ASSERT(children.size() == 3);
	auto &encoding_child = GetStructChild(children[0]);
	auto &run_starts_child = GetStructChild(children[1]);
	auto &values_child = GetStructChild(children[2]);

	for (idx_t i = 0; i < args.size(); i++) {
		auto run_starts_idx = run_starts_data.sel->get_index(i);
		auto values_idx = values_data.sel->get_index(i);

		FlatVector::GetData<string_t>(encoding_child)[i] = StringVector::AddString(encoding_child, "rse");
		FlatVector::GetData<string_t>(run_starts_child)[i] =
		    StringVector::AddString(run_starts_child, run_starts_ptr[run_starts_idx]);
		FlatVector::GetData<string_t>(values_child)[i] = StringVector::AddString(values_child, values_ptr[values_idx]);
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
	if (args.size() == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

void RegisterH5RseFunction(ExtensionLoader &loader) {
	child_list_t<LogicalType> struct_children = {
	    {"encoding", LogicalType::VARCHAR}, {"run_starts", LogicalType::VARCHAR}, {"values", LogicalType::VARCHAR}};

	auto h5_rse = ScalarFunction("h5_rse", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                             LogicalType::STRUCT(struct_children), H5RseFunction);
	loader.RegisterFunction(h5_rse);
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
	loader.RegisterFunction(h5_alias);
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
	loader.RegisterFunction(h5_index);
}

// Cardinality function - informs DuckDB's optimizer of exact row count
static unique_ptr<NodeStatistics> H5ReadCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<H5ReadBindData>();
	return make_uniq<NodeStatistics>(bind_data.total_num_rows);
}

static OperatorPartitionData GetSingleFilePartitionData(const H5ReadLocalState &lstate) {
	D_ASSERT(lstate.position_end >= H5_READ_LOGICAL_PARTITION_SIZE);
	D_ASSERT(lstate.position_end % H5_READ_LOGICAL_PARTITION_SIZE == 0);
	return OperatorPartitionData(lstate.position_end / H5_READ_LOGICAL_PARTITION_SIZE - 1);
}

static OperatorPartitionData H5ReadGetPartitionData(ClientContext &context, TableFunctionGetPartitionInput &input) {
	if (input.partition_info.RequiresPartitionColumns()) {
		throw InternalException("h5_read::GetPartitionData: partition columns not supported");
	}
	auto &lstate = input.local_state->Cast<H5ReadMultiFileLocalState>();
	if (!lstate.active_local_state) {
		return OperatorPartitionData(0);
	}
	auto inner_partition = GetSingleFilePartitionData(*lstate.active_local_state);
	return OperatorPartitionData(lstate.partition_base + inner_partition.batch_index);
}

static virtual_column_map_t H5GetFilenameVirtualColumns(ClientContext &, optional_ptr<FunctionData> bind_data_p) {
	virtual_column_map_t result;
	if (bind_data_p && bind_data_p->Cast<H5ReadBindData>().visible_filename_idx.has_value()) {
		return result;
	}
	result.emplace(MultiFileReader::COLUMN_IDENTIFIER_FILENAME, TableColumn("filename", LogicalType::VARCHAR));
	return result;
}

void RegisterH5ReadFunction(ExtensionLoader &loader) {
	// First argument is filename (VARCHAR), then 1+ dataset paths (VARCHAR or STRUCT for RSE)
	TableFunction h5_read_function("h5_read", {LogicalType::VARCHAR, LogicalType::ANY}, H5ReadScan, H5ReadBind,
	                               H5ReadInit);
	// Allow additional ANY arguments for multiple datasets (VARCHAR or STRUCT from h5_rse())
	h5_read_function.varargs = LogicalType::ANY;
	h5_read_function.named_parameters["filename"] = LogicalType::ANY;
	h5_read_function.named_parameters["swmr"] = LogicalType::BOOLEAN;

	// Predicate pushdown (RSE only): claim filters in bind, build row ranges in init,
	// and scan only matching ranges while keeping DuckDB's post-scan verification.

	// Enable projection pushdown - only read columns that are actually needed
	h5_read_function.projection_pushdown = true;

	// Enable predicate pushdown for RSE columns
	// We claim RSE filters for I/O optimization but keep them for post-scan verification
	// DuckDB applies all filters post-scan (defensive, ensures correctness)
	h5_read_function.pushdown_complex_filter = H5ReadPushdownComplexFilter;

	// Set cardinality function for query optimizer
	h5_read_function.cardinality = H5ReadCardinality;
	h5_read_function.init_local = H5ReadInitLocal;
	h5_read_function.get_partition_data = H5ReadGetPartitionData;
	h5_read_function.get_virtual_columns = H5GetFilenameVirtualColumns;
	loader.RegisterFunction(MultiFileReader::CreateFunctionSet(std::move(h5_read_function)));
}

} // namespace duckdb
