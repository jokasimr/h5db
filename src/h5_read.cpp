#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "h5_raii.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"
#include <utility>
#include <vector>
#include <string>
#include <limits>
#include <variant>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <iostream>

namespace duckdb {

// =============================================================================
// Type-safe index wrappers for projection pushdown
// =============================================================================
// These prevent accidentally mixing global and local column indices.
// Similar to DuckDB's MultiFileLocalIndex / MultiFileGlobalIndex pattern.
//
// INDEXING STRATEGY (DENSE ARRAY):
// - GlobalColumnIdx: Index into bind_data.columns (schema/bind-time indices)
// - LocalColumnIdx: Index into column_states (scan-time dense array [0,1,2...])
// - output.data[i]: Also indexed [0,1,2...] matching LocalColumnIdx
//
// Example with projection SELECT col2, col4 FROM table(col1, col2, col3, col4):
//   columns_to_scan = [1, 3]          // Global indices
//   column_states.size() = 2          // Dense: only 2 elements
//   column_states[0] = state for col2 // Local idx 0 -> Global idx 1
//   column_states[1] = state for col4 // Local idx 1 -> Global idx 3
//   output.data[0] = col2 data
//   output.data[1] = col4 data

struct LocalColumnIdx {
	idx_t index; // Index into column_states [0, 1, 2, ...]
	explicit LocalColumnIdx(idx_t i) : index(i) {
	}
	operator idx_t() const {
		return index;
	}
};

struct GlobalColumnIdx {
	idx_t index; // Index into bind_data.columns
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
	H5TypeHandle h5_type_id; // RAII wrapper - automatically closes on destruction
	bool is_string;
	int ndims;
	std::vector<hsize_t> dims;
	size_t element_size;
};

// Run-Start Encoded column specification
struct RSEColumnSpec {
	std::string run_starts_path;
	std::string values_path;
	std::string column_name;
	LogicalType column_type;
	H5TypeHandle run_starts_h5_type; // HDF5 type for run_starts (determined in Bind)
	H5TypeHandle values_h5_type;     // HDF5 type for values (determined in Bind)
};

// A column can be either regular or RSE
using ColumnSpec = std::variant<RegularColumnSpec, RSEColumnSpec>;

// Chunk cache data (separate struct to allow unique_ptr due to non-movable mutex/cv)
struct Chunk {
	idx_t chunk_size = 0; // Rows per chunk

	// Typed storage - buffer with chunk_size capacity
	using CacheStorage =
	    std::variant<std::monostate, // No cache (for non-cacheable columns)
	                 std::vector<int8_t>, std::vector<int16_t>, std::vector<int32_t>, std::vector<int64_t>,
	                 std::vector<uint8_t>, std::vector<uint16_t>, std::vector<uint32_t>, std::vector<uint64_t>,
	                 std::vector<float>, std::vector<double>,
	                 std::vector<string> // Included for template instantiation (but not actually used for caching)
	                 >;
	CacheStorage cache; // Size: chunk_size elements

	// Chunk state tracking
	// end_row: One past the last row in this chunk. Chunk covers [end_row - chunk_size, end_row)
	// Initialized to 0, which makes chunk appear stale (covers negative range)
	std::atomic<idx_t> end_row {0};
};

struct ChunkCache {
	Chunk chunks[2]; // Fixed size array (atomic members prevent std::vector usage)
};

// Regular column runtime state
struct RegularColumnState {
	H5DatasetHandle dataset;      // RAII wrapper - automatic cleanup
	H5DataspaceHandle file_space; // Cached dataspace handle (reused across chunks)

	// Chunk caching (unique_ptr because mutex/cv are non-movable)
	std::unique_ptr<ChunkCache> chunk_cache;
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

// Runtime state for a column (either regular or RSE)
using ColumnState = std::variant<RegularColumnState, RSEColumnState>;

// Row range for filtering (defined here for use in bind data and global state)
struct RowRange {
	idx_t start_row;
	idx_t end_row;
};

// Filter claimed during pushdown complex filter callback
struct ClaimedFilter {
	idx_t column_index;        // Which column (index into bind_data.columns)
	ExpressionType comparison; // Comparison type (>, <, =, >=, <=)
	Value constant;            // The constant value to compare against
};

// Data for h5_read table function
struct H5ReadBindData : public TableFunctionData {
	std::string filename;
	vector<ColumnSpec> columns;            // Unified column specifications
	hsize_t num_rows;                      // Row count from regular datasets
	vector<ClaimedFilter> claimed_filters; // Filters we claimed during pushdown
};

struct H5ReadGlobalState : public GlobalTableFunctionState {
	H5FileHandle file;                 // RAII wrapper for file handle
	vector<ColumnState> column_states; // DENSE array: indexed by LOCAL position [0, 1, 2, ...]

	// Projection pushdown support
	vector<column_t> columns_to_scan;            // Global column indices (into bind_data.columns)
	unordered_map<idx_t, idx_t> global_to_local; // Maps global column idx -> local column_states idx

	// Position tracking (atomic for lock-free reads in chunk loading)
	std::atomic<idx_t> position;      // This index and forward has not been started yet
	std::atomic<idx_t> position_done; // All rows in [0, position_done) have been returned

	// Row range filtering (for predicate pushdown on RSE columns)
	vector<RowRange> valid_row_ranges; // Sorted, non-overlapping ranges to scan

	// Mutex for thread-safe range selection (enables parallel scanning)
	std::mutex range_selection_mutex;

	// Track out-of-order scan completions for position_done advancement
	// Maps scan start position -> scan end position for completed scans
	// that couldn't be merged into position_done yet (due to gaps)
	std::map<idx_t, idx_t> completed_ranges;

	// Chunk loading coordination: only one thread loads chunks at a time
	// Other threads proceed with scanning cached data (enables parallel processing)
	std::atomic<bool> someone_is_fetching {false};

	H5ReadGlobalState() : position(0), position_done(0) {
	}

	// Override MaxThreads to enable parallel scanning
	idx_t MaxThreads() const override {
		return GlobalTableFunctionState::MAX_THREADS; // Use all available threads
	}

	// No destructor needed - RAII wrappers handle all cleanup automatically
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

// Helper function to build nested array types for multi-dimensional datasets
static LogicalType BuildArrayType(LogicalType base_type, const std::vector<hsize_t> &dims, int ndims) {
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

// Helper function to open a dataset and get its type in one operation
static std::pair<H5DatasetHandle, H5TypeHandle> OpenDatasetAndGetType(hid_t file, const std::string &path) {
	// Open dataset (with error suppression)
	H5DatasetHandle dataset;
	{
		H5ErrorSuppressor suppress;
		dataset = H5DatasetHandle(file, path.c_str());
	}

	if (!dataset.is_valid()) {
		throw IOException("Failed to open dataset: " + path);
	}

	// Get datatype
	hid_t type_id = H5Dget_type(dataset);
	if (type_id < 0) {
		throw IOException("Failed to get dataset type");
	}

	return {std::move(dataset), H5TypeHandle(type_id)};
}

// Helper function to read HDF5 strings (handles both variable-length and fixed-length)
// The callback is called for each string: callback(index, string_value)
static void ReadHDF5Strings(hid_t dataset_id, hid_t h5_type, hid_t mem_space, hid_t file_space, idx_t count,
                            std::function<void(idx_t, const std::string &)> callback) {
	htri_t is_variable = H5Tis_variable_str(h5_type);

	if (is_variable > 0) {
		// Variable-length strings
		std::vector<char *> string_data(count);

		herr_t status = H5Dread(dataset_id, h5_type, mem_space, file_space, H5P_DEFAULT, string_data.data());

		if (status < 0) {
			throw IOException("Failed to read variable-length string data");
		}

		// Process strings via callback
		for (idx_t i = 0; i < count; i++) {
			if (string_data[i]) {
				callback(i, std::string(string_data[i]));
			} else {
				callback(i, std::string());
			}
		}

		// Reclaim HDF5-allocated memory
		hsize_t mem_dim = count;
		H5DataspaceHandle reclaim_space(1, &mem_dim);
		H5Dvlen_reclaim(h5_type, reclaim_space, H5P_DEFAULT, string_data.data());

	} else {
		// Fixed-length strings
		size_t str_len = H5Tget_size(h5_type);
		std::vector<char> buffer(count * str_len);

		herr_t status = H5Dread(dataset_id, h5_type, mem_space, file_space, H5P_DEFAULT, buffer.data());

		if (status < 0) {
			throw IOException("Failed to read fixed-length string data");
		}

		// Process strings via callback
		for (idx_t i = 0; i < count; i++) {
			char *str_ptr = buffer.data() + (i * str_len);
			size_t actual_len = strnlen(str_ptr, str_len);
			callback(i, std::string(str_ptr, actual_len));
		}
	}
}

//===--------------------------------------------------------------------===//
// Predicate Pushdown Helpers (for RSE columns)
//===--------------------------------------------------------------------===//

// Helper: Evaluate a comparison between a value and a filter constant
template <typename T>
static bool EvaluateComparison(const T &value, ExpressionType comparison, const T &filter_val) {
	switch (comparison) {
	case ExpressionType::COMPARE_EQUAL:
		return value == filter_val;
	case ExpressionType::COMPARE_GREATERTHAN:
		return value > filter_val;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return value >= filter_val;
	case ExpressionType::COMPARE_LESSTHAN:
		return value < filter_val;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return value <= filter_val;
	default:
		return false;
	}
}

//===--------------------------------------------------------------------===//
// h5_read - Read datasets from HDF5 files
//===--------------------------------------------------------------------===//

// Bind function - opens datasets and determines schema
static unique_ptr<FunctionData> H5ReadBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<H5ReadBindData>();

	// Get parameters - first is filename, rest are dataset paths or RSE structs
	if (input.inputs.size() < 2) {
		throw IOException("h5_read requires at least 2 arguments: filename and dataset path(s) or h5_rse() calls");
	}

	result->filename = input.inputs[0].GetValue<string>();
	size_t num_columns = input.inputs.size() - 1;

	// Lock for all HDF5 operations (not thread-safe)
	std::lock_guard<std::mutex> lock(hdf5_global_mutex);

	// Open file once (with error suppression) - RAII wrapper handles cleanup
	H5FileHandle file;
	{
		H5ErrorSuppressor suppress;
		file = H5FileHandle(result->filename.c_str(), H5F_ACC_RDONLY);
	}

	if (!file.is_valid()) {
		throw IOException("Failed to open HDF5 file: " + result->filename);
	}

	// Track minimum rows across all regular datasets
	hsize_t min_rows = std::numeric_limits<hsize_t>::max();
	size_t num_regular_datasets = 0;

	// Process each column (regular dataset or RSE)
	for (size_t i = 0; i < num_columns; i++) {
		const auto &input_val = input.inputs[i + 1];

		// Check if this is an RSE column (STRUCT type)
		if (input_val.type().id() == LogicalTypeId::STRUCT) {
			// RSE column - extract struct fields
			auto &children = StructValue::GetChildren(input_val);
			if (children.size() != 3) {
				throw InvalidInputException("h5_rse() must return a struct with 3 fields");
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
			rse_spec.column_name = GetColumnName(values);

			// Open run_starts dataset and get type
			auto [starts_ds, starts_type] = OpenDatasetAndGetType(file, run_starts);
			rse_spec.run_starts_h5_type = std::move(starts_type);

			// Open values dataset and get type
			auto [values_ds, values_type] = OpenDatasetAndGetType(file, values);
			// Determine DuckDB column type from values (before move)
			rse_spec.column_type = H5TypeToDuckDBType(values_type);
			rse_spec.values_h5_type = std::move(values_type);

			result->columns.push_back(std::move(rse_spec));

		} else {
			// Regular dataset
			RegularColumnSpec ds_info;
			ds_info.path = input_val.GetValue<string>();
			ds_info.column_name = GetColumnName(ds_info.path);
			num_regular_datasets++;

			// Open dataset and get type
			auto [dataset, type] = OpenDatasetAndGetType(file, ds_info.path);

			// Check if it's a string type
			ds_info.is_string = (H5Tget_class(type) == H5T_STRING);

			// Get dataspace to determine dimensions - RAII handles cleanup
			H5DataspaceHandle space(dataset);
			if (!space.is_valid()) {
				throw IOException("Failed to get dataset dataspace");
			}

			ds_info.ndims = H5Sget_simple_extent_ndims(space);
			if (ds_info.ndims <= 0) {
				throw IOException("Dataset has no dimensions");
			}

			ds_info.dims.resize(ds_info.ndims);
			H5Sget_simple_extent_dims(space, ds_info.dims.data(), nullptr);

			// Track minimum rows
			if (ds_info.dims[0] < min_rows) {
				min_rows = ds_info.dims[0];
			}

			// Map HDF5 type to DuckDB type
			LogicalType base_type = H5TypeToDuckDBType(type);

			// Calculate element size for multi-dimensional arrays (before move)
			ds_info.element_size = H5Tget_size(type);
			for (int j = 1; j < ds_info.ndims; j++) {
				ds_info.element_size *= ds_info.dims[j];
			}

			// Transfer ownership to ds_info (must be after using type)
			ds_info.h5_type_id = std::move(type);

			// Build array type for multi-dimensional datasets
			ds_info.column_type = BuildArrayType(base_type, ds_info.dims, ds_info.ndims);

			result->columns.push_back(std::move(ds_info));
		}
	}

	// Require at least one regular dataset to determine row count
	if (num_regular_datasets == 0) {
		throw IOException("h5_read requires at least one regular (non-RSE) dataset to determine row count");
	}

	// Set the row count from regular datasets
	result->num_rows = min_rows;

	// Build output schema by iterating through columns in order
	for (const auto &col : result->columns) {
		std::visit(
		    [&](auto &&spec) {
			    names.push_back(spec.column_name);
			    return_types.push_back(spec.column_type);
		    },
		    col);
	}

	return std::move(result);
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

// Init function - open file and dataset for reading
static unique_ptr<GlobalTableFunctionState> H5ReadInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<H5ReadBindData>();
	auto result = make_uniq<H5ReadGlobalState>();

	// Store which columns to scan (projection pushdown)
	if (input.column_ids.empty()) {
		// No projection pushdown - read all columns
		for (idx_t i = 0; i < bind_data.columns.size(); i++) {
			result->columns_to_scan.push_back(i);
		}
	} else {
		result->columns_to_scan = input.column_ids;
	}

	// Build global-to-local index mapping for projection pushdown
	// This allows O(1) lookup: global_column_idx -> local_column_states_idx
	for (idx_t local_idx = 0; local_idx < result->columns_to_scan.size(); local_idx++) {
		idx_t global_idx = result->columns_to_scan[local_idx];
		result->global_to_local[global_idx] = local_idx;
	}

	// Lock for all HDF5 operations (not thread-safe)
	std::lock_guard<std::mutex> lock(hdf5_global_mutex);

	// Open file (with error suppression) - RAII wrapper handles cleanup
	{
		H5ErrorSuppressor suppress;
		result->file = H5FileHandle(bind_data.filename.c_str(), H5F_ACC_RDONLY);
	}

	if (!result->file.is_valid()) {
		throw IOException("Failed to open HDF5 file: " + bind_data.filename);
	}

	// Allocate DENSE column_states array - only for scanned columns
	// Indexed by LOCAL position [0, 1, 2, ...], not global column indices
	result->column_states.reserve(GetNumScannedColumns(*result));

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
					    throw IOException("Failed to open dataset: " + spec.path);
				    }

				    // Cache the file dataspace (reused across all chunks)
				    H5DataspaceHandle file_space(dataset);
				    if (!file_space.is_valid()) {
					    throw IOException("Failed to get dataspace for dataset: " + spec.path);
				    }

				    RegularColumnState state;
				    state.dataset = std::move(dataset);
				    state.file_space = std::move(file_space);

				    // Detect if this column is cacheable (Phase 1: 1D numerical only)
				    bool is_cacheable = (spec.ndims == 1 && !spec.is_string);

				    if (is_cacheable) {
					    // Create cache structure (chunks array is already allocated)
					    state.chunk_cache = std::make_unique<ChunkCache>();

					    // Query HDF5 chunk size or use default
					    idx_t chunk_size = 0;
					    hid_t dcpl = H5Dget_create_plist(state.dataset);
					    if (dcpl >= 0) {
						    H5D_layout_t layout = H5Pget_layout(dcpl);
						    if (layout == H5D_CHUNKED) {
							    hsize_t chunk_dims[1];
							    if (H5Pget_chunk(dcpl, 1, chunk_dims) >= 0) {
								    chunk_size = chunk_dims[0];
							    }
						    }
						    H5Pclose(dcpl);
					    }

					    // If not chunked or error, use default of 1MB / element_size
					    if (chunk_size == 0) {
						    constexpr idx_t DEFAULT_CHUNK_BYTES = 1 * 1024 * 1024; // 4MB
						    chunk_size = DEFAULT_CHUNK_BYTES / spec.element_size;
						    // Ensure at least some minimum chunk size
						    if (chunk_size < 2048) {
							    chunk_size = 2048;
						    }
					    }

					    // Allocate typed cache buffer (chunk_size elements)
					    DispatchOnDuckDBType(spec.column_type, [&](auto type_tag) {
						    using T = typename decltype(type_tag)::type;
						    for (auto &chunk : state.chunk_cache->chunks) {
							    chunk.cache = std::vector<T>(chunk_size);
							    chunk.chunk_size = chunk_size;
						    }
					    });
				    }

				    // Store in dense array with LOCAL indexing
				    result->column_states.push_back(std::move(state));

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
						    throw IOException("Failed to open RSE run_starts dataset: " + spec.run_starts_path);
					    }

					    values_ds = H5DatasetHandle(result->file, spec.values_path.c_str());
					    if (!values_ds.is_valid()) {
						    throw IOException("Failed to open RSE values dataset: " + spec.values_path);
					    }
				    }

				    // Get array sizes
				    H5DataspaceHandle starts_space(starts_ds);
				    hssize_t num_runs_hssize = H5Sget_simple_extent_npoints(starts_space);

				    H5DataspaceHandle values_space(values_ds);
				    hssize_t num_values_hssize = H5Sget_simple_extent_npoints(values_space);

				    if (num_runs_hssize < 0 || num_values_hssize < 0) {
					    throw IOException("Failed to get dataset sizes for RSE column");
				    }

				    size_t num_runs = static_cast<size_t>(num_runs_hssize);
				    size_t num_values = static_cast<size_t>(num_values_hssize);

				    // Validate: run_starts and values must have same size
				    if (num_runs != num_values) {
					    throw IOException("RSE run_starts and values must have same size. Got " +
					                      std::to_string(num_runs) + " and " + std::to_string(num_values));
				    }

				    // Read run_starts - validate it's an integer type, then let HDF5 convert to idx_t (uint64_t)
				    H5T_class_t starts_class = H5Tget_class(spec.run_starts_h5_type);
				    if (starts_class != H5T_INTEGER) {
					    throw IOException("RSE run_starts must be integer type");
				    }

				    // Read directly into run_starts - HDF5 automatically converts from file type to uint64_t
				    rse_col.run_starts.resize(num_runs);
				    herr_t status =
				        H5Dread(starts_ds, H5T_NATIVE_UINT64, H5S_ALL, H5S_ALL, H5P_DEFAULT, rse_col.run_starts.data());

				    if (status < 0) {
					    throw IOException("Failed to read run_starts from: " + spec.run_starts_path);
				    }

				    // Validate run_starts
				    if (num_runs > 0 && rse_col.run_starts[0] != 0) {
					    throw IOException("RSE run_starts must begin with 0, got " +
					                      std::to_string(rse_col.run_starts[0]));
				    }
				    for (size_t i = 1; i < num_runs; i++) {
					    if (rse_col.run_starts[i] <= rse_col.run_starts[i - 1]) {
						    throw IOException("RSE run_starts must be strictly increasing");
					    }
				    }
				    if (num_runs > 0 && rse_col.run_starts.back() >= bind_data.num_rows) {
					    throw IOException("RSE run_starts contains index " + std::to_string(rse_col.run_starts.back()) +
					                      " which exceeds dataset length " + std::to_string(bind_data.num_rows));
				    }

				    // Read values using stored type and type dispatcher
				    // Directly populate typed vector (no Value object overhead)
				    DispatchOnDuckDBType(spec.column_type, [&](auto type_tag) {
					    using T = typename decltype(type_tag)::type;

					    if constexpr (std::is_same_v<T, string>) {
						    // String handling
						    std::vector<string> string_values;
						    string_values.reserve(num_values);
						    ReadHDF5Strings(values_ds, spec.values_h5_type, H5S_ALL, H5S_ALL, num_values,
						                    [&](idx_t i, const std::string &str) { string_values.push_back(str); });
						    rse_col.values = std::move(string_values);
					    } else {
						    // Numeric types: read directly into typed vector
						    std::vector<T> typed_values(num_values);
						    H5Dread(values_ds, spec.values_h5_type, H5S_ALL, H5S_ALL, H5P_DEFAULT, typed_values.data());
						    rse_col.values = std::move(typed_values);
					    }
				    });

				    // Note: RSEColumnState is now stateless (thread-safe)
				    // No runtime state initialization needed

				    // Store in dense array with LOCAL indexing
				    result->column_states.push_back(std::move(rse_col));
			    }
		    },
		    col);
	}

	// Compute row ranges based on claimed filters (from pushdown_complex_filter)
	// Group claimed filters by column
	unordered_map<idx_t, vector<pair<ExpressionType, Value>>> filters_by_column;
	for (const auto &filter : bind_data.claimed_filters) {
		filters_by_column[filter.column_index].push_back({filter.comparison, filter.constant});
	}

	// If we have filters on RSE columns, compute row ranges
	if (!filters_by_column.empty()) {
		vector<RowRange> ranges = {{0, bind_data.num_rows}};

		for (const auto &[global_idx_raw, col_filters] : filters_by_column) {
			// Map global column index to local column_states index
			GlobalColumnIdx global_idx(global_idx_raw);
			LocalColumnIdx local_idx = GlobalToLocal(*result, global_idx);

			auto &rse_spec = std::get<RSEColumnSpec>(bind_data.columns[global_idx]);
			auto &rse_state = std::get<RSEColumnState>(result->column_states[local_idx]);

			// Compute ranges for this column
			vector<RowRange> col_ranges =
			    DispatchOnDuckDBType(rse_spec.column_type, [&](auto type_tag) -> vector<RowRange> {
				    using T = typename decltype(type_tag)::type;
				    auto &typed_values = std::get<std::vector<T>>(rse_state.values);

				    // Convert filters to typed values
				    vector<std::pair<ExpressionType, T>> typed_filters;
				    for (const auto &[comparison, value] : col_filters) {
					    typed_filters.push_back({comparison, value.GetValue<T>()});
				    }

				    // Loop through runs, building ranges where ALL filters are satisfied
				    vector<RowRange> col_result;
				    idx_t current_start = 0;
				    bool in_range = false;

				    for (size_t i = 0; i < typed_values.size(); i++) {
					    const T &value = typed_values[i];
					    idx_t run_start = rse_state.run_starts[i];

					    // Check if this run's value satisfies ALL filters
					    bool satisfies_all = true;
					    for (const auto &[comparison, filter_val] : typed_filters) {
						    if (!EvaluateComparison(value, comparison, filter_val)) {
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
					    col_result.push_back({current_start, bind_data.num_rows});
				    }

				    return col_result;
			    });

			// Intersect ranges from this column with accumulated ranges
			ranges = IntersectRowRanges(ranges, col_ranges);
		}

		result->valid_row_ranges = std::move(ranges);
	} else {
		// No RSE filters - all rows are valid
		result->valid_row_ranges.push_back({0, bind_data.num_rows});
	}

	result->position = 0;
	result->position_done = 0;

	return std::move(result);
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

// Helper: Try to claim a filter on an RSE column
static bool TryClaimRSEFilter(const unique_ptr<Expression> &expr, idx_t table_index,
                              const unordered_map<idx_t, idx_t> &get_to_bind_map,
                              const unordered_set<idx_t> &rse_columns, vector<ClaimedFilter> &claimed) {

	// Handle comparison expressions: col > 10, col = 20, 10 < col, etc.
	if (expr->expression_class == ExpressionClass::BOUND_COMPARISON) {
		auto &comp = expr->Cast<BoundComparisonExpression>();

		const BoundColumnRefExpression *colref = nullptr;
		const BoundConstantExpression *constant = nullptr;
		bool need_flip = false;

		// Determine which side is the column and which is the constant
		if (comp.left->expression_class == ExpressionClass::BOUND_COLUMN_REF &&
		    comp.right->expression_class == ExpressionClass::BOUND_CONSTANT) {
			colref = &comp.left->Cast<BoundColumnRefExpression>();
			constant = &comp.right->Cast<BoundConstantExpression>();
			need_flip = false; // col > 10 is already in correct order
		} else if (comp.left->expression_class == ExpressionClass::BOUND_CONSTANT &&
		           comp.right->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
			colref = &comp.right->Cast<BoundColumnRefExpression>();
			constant = &comp.left->Cast<BoundConstantExpression>();
			need_flip = true; // 10 < col needs to become col > 10
		}

		// If we found a column-constant comparison, try to claim it
		if (colref && constant && colref->binding.table_index == table_index) {
			// Map from LogicalGet column index to bind_data column index
			auto it = get_to_bind_map.find(colref->binding.column_index);
			if (it != get_to_bind_map.end()) {
				idx_t bind_data_col_idx = it->second;

				// Check if this is an RSE column
				if (rse_columns.count(bind_data_col_idx) > 0) {
					ExpressionType comparison = need_flip ? FlipComparison(comp.type) : comp.type;

					// Whitelist: Only claim comparison operators we can optimize with row ranges
					// This matches the operators handled in the row range computation code
					switch (comparison) {
					case ExpressionType::COMPARE_EQUAL:
					case ExpressionType::COMPARE_GREATERTHAN:
					case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
					case ExpressionType::COMPARE_LESSTHAN:
					case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
						// These we can optimize - claim the filter
						ClaimedFilter filter;
						filter.column_index = bind_data_col_idx;
						filter.comparison = comparison;
						filter.constant = constant->value;
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

		// Check if input is an RSE column reference
		if (between.input->expression_class == ExpressionClass::BOUND_COLUMN_REF &&
		    between.lower->expression_class == ExpressionClass::BOUND_CONSTANT &&
		    between.upper->expression_class == ExpressionClass::BOUND_CONSTANT) {

			auto &colref = between.input->Cast<BoundColumnRefExpression>();
			auto &lower_const = between.lower->Cast<BoundConstantExpression>();
			auto &upper_const = between.upper->Cast<BoundConstantExpression>();

			if (colref.binding.table_index == table_index) {
				// Map from LogicalGet column index to bind_data column index
				auto it = get_to_bind_map.find(colref.binding.column_index);
				if (it != get_to_bind_map.end()) {
					idx_t bind_data_col_idx = it->second;

					// Check if this is an RSE column
					if (rse_columns.count(bind_data_col_idx) > 0) {
						// Claim BETWEEN as two filters: col >= lower AND col <= upper
						ClaimedFilter lower_filter;
						lower_filter.column_index = bind_data_col_idx;
						lower_filter.comparison = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
						lower_filter.constant = lower_const.value;
						claimed.push_back(lower_filter);

						ClaimedFilter upper_filter;
						upper_filter.column_index = bind_data_col_idx;
						upper_filter.comparison = ExpressionType::COMPARE_LESSTHANOREQUALTO;
						upper_filter.constant = upper_const.value;
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

		if (conj.type == ExpressionType::CONJUNCTION_AND && conj.children.size() == 2) {
			// Try to claim RSE filters from children
			vector<ClaimedFilter> temp_claimed;
			bool claimed_left =
			    TryClaimRSEFilter(conj.children[0], table_index, get_to_bind_map, rse_columns, temp_claimed);
			bool claimed_right =
			    TryClaimRSEFilter(conj.children[1], table_index, get_to_bind_map, rse_columns, temp_claimed);

			// If we claimed any RSE filters, add them to optimize I/O and return true
			if (claimed_left || claimed_right) {
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

	// Build set of RSE column indices (in bind_data.columns) for quick lookup
	unordered_set<idx_t> rse_column_indices;
	for (idx_t i = 0; i < bind_data.columns.size(); i++) {
		if (std::holds_alternative<RSEColumnSpec>(bind_data.columns[i])) {
			rse_column_indices.insert(i);
		}
	}

	// Map LogicalGet column indices to bind_data column indices using column_ids
	// This is structural (not name-based) and works correctly with projections/aliases
	unordered_map<idx_t, idx_t> get_to_bind_map; // get column idx -> bind_data column idx
	const auto &column_ids = get.GetColumnIds();
	for (idx_t i = 0; i < column_ids.size(); i++) {
		get_to_bind_map[i] = column_ids[i].GetPrimaryIndex();
	}

	idx_t table_index = get.table_index;

	// Claim RSE filters for I/O optimization (but keep them in filter list for post-scan)
	// DuckDB will apply all filters after scan to ensure correctness (defensive approach)
	for (const auto &expr : filters) {
		TryClaimRSEFilter(expr, table_index, get_to_bind_map, rse_column_indices, bind_data.claimed_filters);
	}
}

// Scan function - read data chunks

// Helper structure for range selection results
struct RangeSelection {
	bool has_data;  // False if no more data to read
	idx_t position; // Starting position to read from
	idx_t to_read;  // Number of rows to read
};

static RangeSelection NextRangeFrom(const std::vector<RowRange> &valid_row_ranges, idx_t position) {
	for (const auto &range : valid_row_ranges) {
		if (position < range.end_row) {
			idx_t start = position;
			idx_t remains = MinValue<idx_t>(STANDARD_VECTOR_SIZE, range.end_row - position);
			return {true, start, remains};
		}
		// if position == range.end_row, loop will move to next range automatically
	}
	return {false, 0, 0}; // no range after position
}

// Helper function to determine the next data range to read
static RangeSelection GetNextDataRange(H5ReadGlobalState &gstate) {
	// Thread-safe range selection for parallel scanning
	std::lock_guard<std::mutex> lock(gstate.range_selection_mutex);
	auto range = NextRangeFrom(gstate.valid_row_ranges, gstate.position.load());

	if (range.has_data) {
		// Advance position for next thread (critical for parallel scanning!)
		gstate.position.store(gstate.position.load() + range.to_read);
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

		// THREAD-SAFE: Find which run contains this position (binary search)
		// Don't modify state - multiple threads may call this simultaneously!
		auto it = std::upper_bound(state.run_starts.begin(), state.run_starts.end(), position);
		idx_t current_run = (it - state.run_starts.begin()) - 1;
		idx_t next_run_start =
		    (current_run + 1 < state.run_starts.size()) ? state.run_starts[current_run + 1] : num_rows;

		// OPTIMIZATION: Check if entire chunk belongs to single run
		// With avg run length ~10k and chunk size 2048, this is true ~83% of the time!
		idx_t rows_in_current_run = next_run_start - position;
		if (rows_in_current_run >= to_read) {
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
					result_data[i + j] = StringVector::AddString(result_vector, run_value);
				}
			} else {
				// Numeric types: direct assignment - compiler can vectorize this!
				auto result_data = FlatVector::GetData<T>(result_vector);
				for (idx_t j = 0; j < rows_to_fill; j++) {
					result_data[i + j] = run_value;
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
static void ReadIntoTypedCache(Chunk::CacheStorage &cache, idx_t buffer_offset, hid_t dataset_id, hid_t file_space_id,
                               idx_t dataset_row_start, idx_t rows_to_read, LogicalType column_type) {
	DispatchOnDuckDBType(column_type, [&](auto type_tag) {
		using T = typename decltype(type_tag)::type;
		auto &typed_cache = std::get<std::vector<T>>(cache);

		// Lock for all HDF5 operations (not thread-safe)
		std::lock_guard<std::mutex> lock(hdf5_global_mutex);

		// Select hyperslab in file
		hsize_t start[1] = {dataset_row_start};
		hsize_t count[1] = {rows_to_read};
		H5Sselect_hyperslab(file_space_id, H5S_SELECT_SET, start, nullptr, count, nullptr);

		// Create memory dataspace
		hsize_t mem_dims[1] = {rows_to_read};
		H5DataspaceHandle mem_space(1, mem_dims);

		// Determine H5 type
		hid_t h5_type;
		if constexpr (std::is_same_v<T, int8_t>) {
			h5_type = H5T_NATIVE_INT8;
		} else if constexpr (std::is_same_v<T, int16_t>) {
			h5_type = H5T_NATIVE_INT16;
		} else if constexpr (std::is_same_v<T, int32_t>) {
			h5_type = H5T_NATIVE_INT32;
		} else if constexpr (std::is_same_v<T, int64_t>) {
			h5_type = H5T_NATIVE_INT64;
		} else if constexpr (std::is_same_v<T, uint8_t>) {
			h5_type = H5T_NATIVE_UINT8;
		} else if constexpr (std::is_same_v<T, uint16_t>) {
			h5_type = H5T_NATIVE_UINT16;
		} else if constexpr (std::is_same_v<T, uint32_t>) {
			h5_type = H5T_NATIVE_UINT32;
		} else if constexpr (std::is_same_v<T, uint64_t>) {
			h5_type = H5T_NATIVE_UINT64;
		} else if constexpr (std::is_same_v<T, float>) {
			h5_type = H5T_NATIVE_FLOAT;
		} else if constexpr (std::is_same_v<T, double>) {
			h5_type = H5T_NATIVE_DOUBLE;
		}

		// Read into cache buffer at specified offset
		herr_t status =
		    H5Dread(dataset_id, h5_type, mem_space, file_space_id, H5P_DEFAULT, typed_cache.data() + buffer_offset);
		if (status < 0) {
			throw IOException("Failed to read chunk from HDF5 dataset");
		}
	});
}

// Helper: Copy data from typed cache to result vector
static void CopyFromTypedCache(const Chunk::CacheStorage &cache, idx_t buffer_offset, idx_t rows_to_copy,
                               Vector &result_vector, idx_t result_offset, LogicalType column_type) {
	DispatchOnDuckDBType(column_type, [&](auto type_tag) {
		using T = typename decltype(type_tag)::type;
		const auto &typed_cache = std::get<std::vector<T>>(cache);

		// Get result data pointer
		auto result_data = FlatVector::GetData<T>(result_vector);

		// Copy from cache to result
		std::memcpy(result_data + result_offset, typed_cache.data() + buffer_offset, rows_to_copy * sizeof(T));
	});
}

static void TryLoadChunks(ChunkCache &cache, hid_t dataset_id, hid_t file_space_id,
                          const std::vector<RowRange> &valid_row_ranges, std::atomic<idx_t> &position_done,
                          idx_t total_rows, LogicalType column_type) {

	idx_t max_end_row = 0;
	for (Chunk &chunk : cache.chunks) {
		auto end_row = chunk.end_row.load(std::memory_order_acquire);
		max_end_row = end_row > max_end_row ? end_row : max_end_row;
	}
	for (Chunk &chunk : cache.chunks) {

		if (chunk.end_row.load(std::memory_order_acquire) <= position_done.load(std::memory_order_acquire)) {
			// Chunk is finished
			auto next_range = NextRangeFrom(valid_row_ranges, max_end_row);
			if (next_range.has_data) {

				idx_t rows_to_load = std::min(chunk.chunk_size, total_rows - next_range.position);
				ReadIntoTypedCache(chunk.cache, 0, dataset_id, file_space_id, next_range.position, rows_to_load,
				                   column_type);

				idx_t new_end = next_range.position + chunk.chunk_size;
				chunk.end_row.store(new_end, std::memory_order_release);
				chunk.end_row.notify_all();

				max_end_row = new_end;
			}
		}
	}
}

static void TryRefreshCache(H5ReadGlobalState &gstate, const H5ReadBindData &bind_data) {
	bool expected = false;
	if (gstate.someone_is_fetching.compare_exchange_strong(expected, true)) {

		// Only refresh cache for columns being scanned (projection pushdown)
		// Uses LOCAL indexing (dense array)
		for (idx_t i = 0; i < GetNumScannedColumns(gstate); i++) {
			LocalColumnIdx local_idx(i);
			GlobalColumnIdx global_idx = GetGlobalIdx(gstate, local_idx);
			const auto &col_spec = bind_data.columns[global_idx];
			auto &col_state = gstate.column_states[local_idx];

			std::visit(
			    [&](auto &&spec, auto &&state) {
				    using SpecT = std::decay_t<decltype(spec)>;
				    using StateT = std::decay_t<decltype(state)>;

				    if constexpr (std::is_same_v<SpecT, RegularColumnSpec> &&
				                  std::is_same_v<StateT, RegularColumnState>) {
					    if (state.chunk_cache) {
						    TryLoadChunks(*state.chunk_cache, state.dataset.get(), state.file_space.get(),
						                  gstate.valid_row_ranges, gstate.position_done, bind_data.num_rows,
						                  spec.column_type);
					    }
				    }
			    },
			    col_spec, col_state);
		}
		// Done loading - release the flag so another thread can load next time
		gstate.someone_is_fetching.store(false);
		gstate.someone_is_fetching.notify_all();
	}
}

// Helper function to scan a regular dataset column
static void ScanRegularColumn(const RegularColumnSpec &spec, RegularColumnState &state, Vector &result_vector,
                              idx_t position, idx_t to_read, const H5ReadBindData &bind_data,
                              H5ReadGlobalState &gstate) {
	// Check if using cache
	if (state.chunk_cache) {
		auto &cache = *state.chunk_cache;
		auto *chunk1 = &cache.chunks[0];
		auto *chunk2 = &cache.chunks[1];

		for (idx_t i = 0;; i++) {

			if (i > 0) {
				TryRefreshCache(gstate, bind_data);
			}

			idx_t end1 = chunk1->end_row.load(std::memory_order_acquire);
			idx_t end2 = chunk2->end_row.load(std::memory_order_acquire);

			if (end1 > end2) {
				std::swap(chunk1, chunk2);
				std::swap(end1, end2);
			}

			if (position + to_read <= end2) {
				break;
			}

			if (gstate.someone_is_fetching.load(std::memory_order_acquire)) {
				chunk1->end_row.wait(end1, std::memory_order_relaxed);
			}
		}

		// Copy data from chunks that overlap our read range [position, position + to_read)
		for (Chunk *chunk : {chunk1, chunk2}) {
			idx_t chunk_end = chunk->end_row.load(std::memory_order_acquire);
			idx_t chunk_start = (chunk_end > chunk->chunk_size) ? (chunk_end - chunk->chunk_size) : 0;

			// Check if chunk overlaps our range
			if (chunk_start < position + to_read && chunk_end > position) {
				idx_t overlap_start = MaxValue<idx_t>(chunk_start, position);
				idx_t overlap_end = MinValue<idx_t>(chunk_end, position + to_read);
				idx_t overlap_size = overlap_end - overlap_start;

				idx_t chunk_offset = overlap_start - chunk_start;
				idx_t result_offset = overlap_start - position;

				CopyFromTypedCache(chunk->cache, chunk_offset, overlap_size, result_vector, result_offset,
				                   spec.column_type);
			}
		}

		return; // Done with cached read
	}

	std::lock_guard<std::mutex> lock(hdf5_global_mutex);

	// Non-cached path: fall back to direct HDF5 read
	// Access RAII-wrapped handles from state
	hid_t dataset_id = state.dataset.get();
	hid_t file_space = state.file_space.get();

	// Create memory dataspace for reading
	H5DataspaceHandle mem_space; // RAII wrapper - automatic cleanup

	if (spec.ndims == 1) {
		// 1D dataset
		hsize_t mem_dims[1] = {to_read};
		mem_space = H5DataspaceHandle(1, mem_dims);

		hsize_t start[1] = {position};
		hsize_t count[1] = {to_read};
		H5Sselect_hyperslab(file_space, H5S_SELECT_SET, start, nullptr, count, nullptr);
	} else {
		// Multi-dimensional dataset
		// Create memory space with same dimensionality as file
		std::vector<hsize_t> mem_dims(spec.ndims);
		mem_dims[0] = to_read;
		for (int i = 1; i < spec.ndims; i++) {
			mem_dims[i] = spec.dims[i];
		}
		mem_space = H5DataspaceHandle(spec.ndims, mem_dims.data());

		// Select hyperslab from file
		std::vector<hsize_t> start(spec.ndims, 0);
		std::vector<hsize_t> count(spec.ndims);
		start[0] = position;
		count[0] = to_read;
		for (int i = 1; i < spec.ndims; i++) {
			count[i] = spec.dims[i];
		}
		H5Sselect_hyperslab(file_space, H5S_SELECT_SET, start.data(), nullptr, count.data(), nullptr);
	}

	// Read data based on type
	if (spec.is_string) {
		// Handle string data using helper
		ReadHDF5Strings(
		    dataset_id, spec.h5_type_id, mem_space, file_space, to_read, [&](idx_t i, const std::string &str) {
			    if (str.empty()) {
				    FlatVector::SetNull(result_vector, i, true);
			    } else {
				    FlatVector::GetData<string_t>(result_vector)[i] = StringVector::AddString(result_vector, str);
			    }
		    });

	} else {
		// Handle numeric data
		if (spec.ndims == 1) {
			// 1D dataset: read directly into DuckDB vector
			// Use type dispatcher to get typed pointer and read data
			herr_t status = DispatchOnDuckDBType(spec.column_type, [&](auto type_tag) {
				using T = typename decltype(type_tag)::type;
				void *data_ptr = FlatVector::GetData<T>(result_vector);
				return H5Dread(dataset_id, spec.h5_type_id, mem_space, file_space, H5P_DEFAULT, data_ptr);
			});

			if (status < 0) {
				throw IOException("Failed to read data from dataset: " + spec.path);
			}

		} else {
			// Multi-dimensional dataset: read into buffer, then populate arrays
			// For arrays in DuckDB, data is stored contiguously in the innermost child vector
			Vector *current_vector = &result_vector;
			LogicalType current_type = spec.column_type;

			// Navigate through nested array levels to get to the innermost vector
			while (current_type.id() == LogicalTypeId::ARRAY) {
				current_vector = &ArrayVector::GetEntry(*current_vector);
				current_type = ArrayType::GetChildType(current_type);
			}

			// Get pointer to the innermost child data and read using type dispatcher
			herr_t status = DispatchOnDuckDBType(current_type, [&](auto type_tag) {
				using T = typename decltype(type_tag)::type;
				void *child_data = FlatVector::GetData<T>(*current_vector);
				return H5Dread(dataset_id, spec.h5_type_id, mem_space, file_space, H5P_DEFAULT, child_data);
			});

			if (status < 0) {
				throw IOException("Failed to read data from dataset: " + spec.path);
			}
		}
	}

	// Note: file_space is cached and will be closed in destructor
}

static void H5ReadScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<H5ReadBindData>();
	auto &gstate = data.global_state->Cast<H5ReadGlobalState>();

	// Step 2: Determine next data range to read
	auto range_selection = GetNextDataRange(gstate);
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

		auto &result_vector = output.data[i];                 // Sequential output
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

			    } else if constexpr (std::is_same_v<SpecT, RegularColumnSpec> &&
			                         std::is_same_v<StateT, RegularColumnState>) {
				    // Regular dataset - call helper function
				    ScanRegularColumn(spec, state, result_vector, position, to_read, bind_data, gstate);
			    }
		    },
		    col_spec, col_state);
	}

	output.SetCardinality(to_read);

	// Update position_done to track completed scans
	// This scan returned rows [position, position + to_read)
	// position_done uses half-open interval: [0, position_done) has been returned
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
				auto it = gstate.completed_ranges.find(current_done);
				if (it == gstate.completed_ranges.end()) {
					break;
				}
				gstate.position_done.store(it->second);
				gstate.completed_ranges.erase(it);
			}
		} else {
			// This scan completed out of order - store for later merging
			gstate.completed_ranges[position] = scan_end;
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

	for (idx_t i = 0; i < args.size(); i++) {
		auto run_starts_idx = run_starts_data.sel->get_index(i);
		auto values_idx = values_data.sel->get_index(i);

		FlatVector::GetData<string_t>(*children[0])[i] = StringVector::AddString(*children[0], "rse");
		FlatVector::GetData<string_t>(*children[1])[i] =
		    StringVector::AddString(*children[1], run_starts_ptr[run_starts_idx]);
		FlatVector::GetData<string_t>(*children[2])[i] = StringVector::AddString(*children[2], values_ptr[values_idx]);
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

// Cardinality function - informs DuckDB's optimizer of exact row count
static unique_ptr<NodeStatistics> H5ReadCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<H5ReadBindData>();
	return make_uniq<NodeStatistics>(bind_data.num_rows);
}

void RegisterH5ReadFunction(ExtensionLoader &loader) {
	// First argument is filename (VARCHAR), then 1+ dataset paths (VARCHAR or STRUCT for RSE)
	TableFunction h5_read("h5_read", {LogicalType::VARCHAR, LogicalType::ANY}, H5ReadScan, H5ReadBind, H5ReadInit);
	h5_read.name = "h5_read";
	// Allow additional ANY arguments for multiple datasets (VARCHAR or STRUCT from h5_rse())
	h5_read.varargs = LogicalType::ANY;

	// ===================================================================================
	// PREDICATE PUSHDOWN: Enabled for RSE columns
	// ===================================================================================
	// Implementation approach:
	//   1. Bind time (pushdown_complex_filter callback):
	//      - Parse Expression objects to identify filters on RSE columns
	//      - Claim filters on RSE columns (store in bind_data) for I/O optimization
	//      - Keep all filters in filter list for DuckDB to apply post-scan (defensive)
	//      - Principle: filtering is cheap, reading data is expensive
	//
	//   2. Init time (H5ReadInit):
	//      - Load RSE data for all RSE columns
	//      - For each RSE column with filters:
	//        * Loop through runs checking if value satisfies ALL filters on that column
	//        * Build sorted, non-overlapping row ranges using state machine
	//        * Works for both sorted and unsorted RSE columns!
	//      - Store final row ranges in global state
	//
	//   3. Scan time (H5ReadScan):
	//      - Iterate through row ranges, reading only matching rows
	//      - Achieves I/O reduction even for unsorted RSE columns
	//
	// Benefits:
	//   - Works for both sorted and unsorted RSE columns (linear scan approach)
	//   - Row ranges automatically non-overlapping and sorted by construction
	//   - Supports multiple filters on same column (AND conjunction)
	//   - Handles mixed RSE and regular column filters correctly
	// ===================================================================================

	// Enable projection pushdown - only read columns that are actually needed
	h5_read.projection_pushdown = true;

	// Enable predicate pushdown for RSE columns
	// We claim RSE filters for I/O optimization but keep them for post-scan verification
	// DuckDB applies all filters post-scan (defensive, ensures correctness)
	h5_read.pushdown_complex_filter = H5ReadPushdownComplexFilter;

	// Set cardinality function for query optimizer
	h5_read.cardinality = H5ReadCardinality;

	loader.RegisterFunction(h5_read);
}

} // namespace duckdb
