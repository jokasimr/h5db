#include "h5_functions.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"
#include <vector>
#include <string>
#include <limits>
#include <variant>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace duckdb {

// Global mutex for all HDF5 operations
// HDF5 is not guaranteed to be thread-safe, so we serialize all HDF5 calls
// This prevents crashes when DuckDB parallelizes table function execution
static std::mutex hdf5_global_mutex;

// RAII wrapper for HDF5 error suppression
// Automatically disables HDF5 error printing on construction and restores it on destruction
class H5ErrorSuppressor {
	H5E_auto2_t old_func;
	void *old_client_data;

public:
	H5ErrorSuppressor() {
		H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
		H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);
	}

	~H5ErrorSuppressor() {
		H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);
	}

	// Disable copy and move to prevent double restoration
	H5ErrorSuppressor(const H5ErrorSuppressor &) = delete;
	H5ErrorSuppressor &operator=(const H5ErrorSuppressor &) = delete;
	H5ErrorSuppressor(H5ErrorSuppressor &&) = delete;
	H5ErrorSuppressor &operator=(H5ErrorSuppressor &&) = delete;
};

// RAII wrapper for HDF5 type handles
// Automatically closes type handle on destruction
class H5TypeHandle {
	hid_t id;

public:
	H5TypeHandle() : id(-1) {
	}

	explicit H5TypeHandle(hid_t type_id) : id(H5Tcopy(type_id)) {
		if (id < 0) {
			throw IOException("Failed to copy HDF5 type");
		}
	}

	~H5TypeHandle() {
		if (id >= 0) {
			H5Tclose(id);
		}
	}

	// Allow conversion to hid_t for use in HDF5 functions
	operator hid_t() const {
		return id;
	}
	hid_t get() const {
		return id;
	}

	// Disable copy but enable move
	H5TypeHandle(const H5TypeHandle &) = delete;
	H5TypeHandle &operator=(const H5TypeHandle &) = delete;

	H5TypeHandle(H5TypeHandle &&other) noexcept : id(other.id) {
		other.id = -1;
	}

	H5TypeHandle &operator=(H5TypeHandle &&other) noexcept {
		if (this != &other) {
			if (id >= 0) {
				H5Tclose(id);
			}
			id = other.id;
			other.id = -1;
		}
		return *this;
	}
};

// Type tag for compile-time type dispatch
template <typename T>
struct TypeTag {
	using type = T;
};

// Type dispatcher - centralizes all type switching logic
// Takes a DuckDB LogicalType and a lambda/function that accepts TypeTag<T>
template <typename Func>
auto DispatchOnDuckDBType(LogicalType logical_type, Func &&func) {
	switch (logical_type.id()) {
	case LogicalTypeId::TINYINT:
		return func(TypeTag<int8_t> {});
	case LogicalTypeId::SMALLINT:
		return func(TypeTag<int16_t> {});
	case LogicalTypeId::INTEGER:
		return func(TypeTag<int32_t> {});
	case LogicalTypeId::BIGINT:
		return func(TypeTag<int64_t> {});
	case LogicalTypeId::UTINYINT:
		return func(TypeTag<uint8_t> {});
	case LogicalTypeId::USMALLINT:
		return func(TypeTag<uint16_t> {});
	case LogicalTypeId::UINTEGER:
		return func(TypeTag<uint32_t> {});
	case LogicalTypeId::UBIGINT:
		return func(TypeTag<uint64_t> {});
	case LogicalTypeId::FLOAT:
		return func(TypeTag<float> {});
	case LogicalTypeId::DOUBLE:
		return func(TypeTag<double> {});
	case LogicalTypeId::VARCHAR:
		return func(TypeTag<string> {});
	default:
		throw IOException("Unsupported DuckDB type");
	}
}

// Convert HDF5 type to string representation
std::string H5TypeToString(hid_t type_id) {
	H5T_class_t type_class = H5Tget_class(type_id);
	size_t size = H5Tget_size(type_id);
	H5T_sign_t sign = H5T_SGN_ERROR;

	switch (type_class) {
	case H5T_INTEGER:
		sign = H5Tget_sign(type_id);
		if (sign == H5T_SGN_NONE) {
			// Unsigned integer
			switch (size) {
			case 1:
				return "uint8";
			case 2:
				return "uint16";
			case 4:
				return "uint32";
			case 8:
				return "uint64";
			default:
				return "uint" + std::to_string(size * 8);
			}
		} else {
			// Signed integer
			switch (size) {
			case 1:
				return "int8";
			case 2:
				return "int16";
			case 4:
				return "int32";
			case 8:
				return "int64";
			default:
				return "int" + std::to_string(size * 8);
			}
		}
	case H5T_FLOAT:
		switch (size) {
		case 2:
			return "float16";
		case 4:
			return "float32";
		case 8:
			return "float64";
		default:
			return "float" + std::to_string(size * 8);
		}
	case H5T_STRING:
		return "string";
	case H5T_COMPOUND:
		return "compound";
	case H5T_ENUM:
		return "enum";
	case H5T_ARRAY:
		return "array";
	default:
		return "unknown";
	}
}

// Get dataset shape as string (e.g., "(10,)" or "(5, 4, 3)")
std::string H5GetShapeString(hid_t dataset_id) {
	hid_t space_id = H5Dget_space(dataset_id);
	if (space_id < 0) {
		return "()";
	}

	int ndims = H5Sget_simple_extent_ndims(space_id);
	if (ndims <= 0) {
		H5Sclose(space_id);
		return "()";
	}

	std::vector<hsize_t> dims(ndims);
	H5Sget_simple_extent_dims(space_id, dims.data(), nullptr);
	H5Sclose(space_id);

	std::string result = "(";
	for (int i = 0; i < ndims; i++) {
		if (i > 0)
			result += ", ";
		result += std::to_string(dims[i]);
	}
	result += ")";

	return result;
}

// ==================== h5_tree Implementation ====================

struct H5ObjectInfo {
	std::string path;
	std::string type;  // "group" or "dataset"
	std::string dtype; // data type (for datasets)
	std::string shape; // shape (for datasets)
};

// Data for the h5_tree table function
struct H5TreeBindData : public TableFunctionData {
	std::string filename;
	mutable std::vector<H5ObjectInfo> objects;
	mutable bool scanned = false;
};

struct H5TreeGlobalState : public GlobalTableFunctionState {
	idx_t position = 0;
};

// Callback function for H5Ovisit
static herr_t visit_callback(hid_t obj_id, const char *name, const H5O_info_t *info, void *op_data) {
	auto &objects = *reinterpret_cast<std::vector<H5ObjectInfo> *>(op_data);

	H5ObjectInfo obj_info;
	obj_info.path = std::string("/") + name;

	if (info->type == H5O_TYPE_GROUP) {
		obj_info.type = "group";
		obj_info.dtype = "";
		obj_info.shape = "";
	} else if (info->type == H5O_TYPE_DATASET) {
		obj_info.type = "dataset";

		// Open the dataset to get its properties
		hid_t dataset_id = H5Dopen2(obj_id, name, H5P_DEFAULT);
		if (dataset_id >= 0) {
			// Get datatype
			hid_t type_id = H5Dget_type(dataset_id);
			if (type_id >= 0) {
				obj_info.dtype = H5TypeToString(type_id);
				H5Tclose(type_id);
			}

			// Get shape
			obj_info.shape = H5GetShapeString(dataset_id);

			H5Dclose(dataset_id);
		}
	}

	objects.push_back(obj_info);
	return 0; // Continue iteration
}

// Bind function - opens the file and scans it
static unique_ptr<FunctionData> H5TreeBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<H5TreeBindData>();

	// Get filename parameter
	result->filename = input.inputs[0].GetValue<string>();

	// Define output schema
	names = {"path", "type", "dtype", "shape"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};

	return std::move(result);
}

// Init function - scan the file if not already scanned
static unique_ptr<GlobalTableFunctionState> H5TreeInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<H5TreeBindData>();
	auto result = make_uniq<H5TreeGlobalState>();

	if (!bind_data.scanned) {
		// Lock for all HDF5 operations (not thread-safe)
		std::lock_guard<std::mutex> lock(hdf5_global_mutex);

		// Open the HDF5 file (with error suppression)
		hid_t file_id;
		{
			H5ErrorSuppressor suppress;
			file_id = H5Fopen(bind_data.filename.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
		}

		if (file_id < 0) {
			throw IOException("Failed to open HDF5 file: " + bind_data.filename);
		}

		// Visit all objects in the file
		H5O_info_t obj_info;
		if (H5Oget_info(file_id, &obj_info, H5O_INFO_BASIC) >= 0) {
			// Visit all objects recursively
			H5Ovisit(file_id, H5_INDEX_NAME, H5_ITER_NATIVE, visit_callback, &bind_data.objects, H5O_INFO_BASIC);
		}

		H5Fclose(file_id);
		bind_data.scanned = true;
	}

	return std::move(result);
}

// Scan function - return rows
static void H5TreeScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<H5TreeBindData>();
	auto &gstate = data.global_state->Cast<H5TreeGlobalState>();

	idx_t count = 0;
	idx_t remaining = bind_data.objects.size() - gstate.position;
	idx_t to_process = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);

	if (to_process == 0) {
		output.SetCardinality(0);
		return;
	}

	// Fill the output chunk
	auto &path_vector = output.data[0];
	auto &type_vector = output.data[1];
	auto &dtype_vector = output.data[2];
	auto &shape_vector = output.data[3];

	for (idx_t i = 0; i < to_process; i++) {
		const auto &obj = bind_data.objects[gstate.position + i];

		FlatVector::GetData<string_t>(path_vector)[i] = StringVector::AddString(path_vector, obj.path);
		FlatVector::GetData<string_t>(type_vector)[i] = StringVector::AddString(type_vector, obj.type);
		FlatVector::GetData<string_t>(dtype_vector)[i] = StringVector::AddString(dtype_vector, obj.dtype);
		FlatVector::GetData<string_t>(shape_vector)[i] = StringVector::AddString(shape_vector, obj.shape);

		count++;
	}

	gstate.position += count;
	output.SetCardinality(count);
}

void RegisterH5TreeFunction(ExtensionLoader &loader) {
	TableFunction h5_tree("h5_tree", {LogicalType::VARCHAR}, H5TreeScan, H5TreeBind, H5TreeInit);
	h5_tree.name = "h5_tree";

	loader.RegisterFunction(h5_tree);
}

// ==================== h5_rse Implementation ====================

// h5_rse() - Creates a struct indicating a run-start encoded column
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

// ==================== h5_read Implementation ====================

// Map HDF5 type to DuckDB LogicalType
LogicalType H5TypeToDuckDBType(hid_t type_id) {
	H5T_class_t type_class = H5Tget_class(type_id);
	size_t size = H5Tget_size(type_id);

	switch (type_class) {
	case H5T_INTEGER: {
		H5T_sign_t sign = H5Tget_sign(type_id);
		if (sign == H5T_SGN_NONE) {
			// Unsigned integer
			switch (size) {
			case 1:
				return LogicalType::UTINYINT;
			case 2:
				return LogicalType::USMALLINT;
			case 4:
				return LogicalType::UINTEGER;
			case 8:
				return LogicalType::UBIGINT;
			default:
				throw IOException("Unsupported unsigned integer size: " + std::to_string(size));
			}
		} else {
			// Signed integer
			switch (size) {
			case 1:
				return LogicalType::TINYINT;
			case 2:
				return LogicalType::SMALLINT;
			case 4:
				return LogicalType::INTEGER;
			case 8:
				return LogicalType::BIGINT;
			default:
				throw IOException("Unsupported signed integer size: " + std::to_string(size));
			}
		}
	}
	case H5T_FLOAT:
		switch (size) {
		case 4:
			return LogicalType::FLOAT;
		case 8:
			return LogicalType::DOUBLE;
		default:
			throw IOException("Unsupported float size: " + std::to_string(size));
		}
	case H5T_STRING:
		return LogicalType::VARCHAR;
	default:
		throw IOException("Unsupported HDF5 type class: " + std::to_string(type_class));
	}
}

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

// Regular column runtime state
struct RegularColumnState {
	hid_t dataset_id;
	hid_t file_space; // Cached dataspace handle (reused across chunks)
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

	idx_t current_run;
	idx_t next_run_start;
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
	hid_t file_id;
	vector<ColumnState> column_states; // Unified column states
	idx_t position;

	// Row range filtering (for predicate pushdown on RSE columns)
	vector<RowRange> valid_row_ranges; // Sorted, non-overlapping ranges to scan
	idx_t current_range_idx = 0;       // Which range we're currently scanning

	H5ReadGlobalState() : file_id(-1), position(0) {
	}

	~H5ReadGlobalState() {
		// Close all regular column datasets and cached dataspaces
		for (auto &state : column_states) {
			if (auto *reg_state = std::get_if<RegularColumnState>(&state)) {
				if (reg_state->file_space >= 0) {
					H5Sclose(reg_state->file_space);
				}
				if (reg_state->dataset_id >= 0) {
					H5Dclose(reg_state->dataset_id);
				}
			}
		}
		if (file_id >= 0) {
			H5Fclose(file_id);
		}
	}
};

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

//===--------------------------------------------------------------------===//
// Predicate Pushdown Helpers (for RSE columns)
//===--------------------------------------------------------------------===//

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

	// Open file once (with error suppression)
	hid_t file_id;
	{
		H5ErrorSuppressor suppress;
		file_id = H5Fopen(result->filename.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
	}

	if (file_id < 0) {
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

			// Open run_starts dataset to determine type (with error suppression)
			hid_t starts_ds;
			{
				H5ErrorSuppressor suppress;
				starts_ds = H5Dopen2(file_id, run_starts.c_str(), H5P_DEFAULT);
			}

			if (starts_ds < 0) {
				H5Fclose(file_id);
				throw IOException("Failed to open RSE run_starts dataset: " + run_starts);
			}

			hid_t starts_type = H5Dget_type(starts_ds);
			if (starts_type < 0) {
				H5Dclose(starts_ds);
				H5Fclose(file_id);
				throw IOException("Failed to get type for RSE run_starts dataset: " + run_starts);
			}

			// Store run_starts type (H5TypeHandle will copy and manage it)
			rse_spec.run_starts_h5_type = H5TypeHandle(starts_type);

			H5Tclose(starts_type);
			H5Dclose(starts_ds);

			// Open values dataset to determine type (with error suppression)
			hid_t values_ds;
			{
				H5ErrorSuppressor suppress;
				values_ds = H5Dopen2(file_id, values.c_str(), H5P_DEFAULT);
			}

			if (values_ds < 0) {
				H5Fclose(file_id);
				throw IOException("Failed to open RSE values dataset: " + values);
			}

			hid_t values_type = H5Dget_type(values_ds);
			if (values_type < 0) {
				H5Dclose(values_ds);
				H5Fclose(file_id);
				throw IOException("Failed to get type for RSE values dataset: " + values);
			}

			// Store values type (H5TypeHandle will copy and manage it)
			rse_spec.values_h5_type = H5TypeHandle(values_type);

			// Determine DuckDB column type from values
			rse_spec.column_type = H5TypeToDuckDBType(values_type);

			H5Tclose(values_type);
			H5Dclose(values_ds);

			result->columns.push_back(std::move(rse_spec));

		} else {
			// Regular dataset
			RegularColumnSpec ds_info;
			ds_info.path = input_val.GetValue<string>();
			ds_info.column_name = GetColumnName(ds_info.path);
			num_regular_datasets++;

			// Open dataset (with error suppression)
			hid_t dataset_id;
			{
				H5ErrorSuppressor suppress;
				dataset_id = H5Dopen2(file_id, ds_info.path.c_str(), H5P_DEFAULT);
			}

			if (dataset_id < 0) {
				H5Fclose(file_id);
				throw IOException("Failed to open dataset: " + ds_info.path);
			}

			// Get datatype
			hid_t type_id = H5Dget_type(dataset_id);
			if (type_id < 0) {
				H5Dclose(dataset_id);
				H5Fclose(file_id);
				throw IOException("Failed to get dataset type");
			}

			// Check if it's a string type
			ds_info.is_string = (H5Tget_class(type_id) == H5T_STRING);

			// Get dataspace to determine dimensions
			hid_t space_id = H5Dget_space(dataset_id);
			if (space_id < 0) {
				H5Tclose(type_id);
				H5Dclose(dataset_id);
				H5Fclose(file_id);
				throw IOException("Failed to get dataset dataspace");
			}

			ds_info.ndims = H5Sget_simple_extent_ndims(space_id);
			if (ds_info.ndims <= 0) {
				H5Sclose(space_id);
				H5Tclose(type_id);
				H5Dclose(dataset_id);
				H5Fclose(file_id);
				throw IOException("Dataset has no dimensions");
			}

			ds_info.dims.resize(ds_info.ndims);
			H5Sget_simple_extent_dims(space_id, ds_info.dims.data(), nullptr);
			H5Sclose(space_id);

			// Track minimum rows
			if (ds_info.dims[0] < min_rows) {
				min_rows = ds_info.dims[0];
			}

			// Map HDF5 type to DuckDB type
			LogicalType base_type = H5TypeToDuckDBType(type_id);
			ds_info.h5_type_id = H5TypeHandle(type_id); // RAII wrapper - copies and auto-closes

			// Calculate element size for multi-dimensional arrays
			ds_info.element_size = H5Tget_size(type_id);
			for (int j = 1; j < ds_info.ndims; j++) {
				ds_info.element_size *= ds_info.dims[j];
			}

			// Build array type for multi-dimensional datasets
			if (ds_info.ndims == 1) {
				ds_info.column_type = base_type;
			} else if (ds_info.ndims == 2) {
				ds_info.column_type = LogicalType::ARRAY(base_type, ds_info.dims[1]);
			} else if (ds_info.ndims == 3) {
				auto inner = LogicalType::ARRAY(base_type, ds_info.dims[2]);
				ds_info.column_type = LogicalType::ARRAY(inner, ds_info.dims[1]);
			} else if (ds_info.ndims == 4) {
				auto innermost = LogicalType::ARRAY(base_type, ds_info.dims[3]);
				auto middle = LogicalType::ARRAY(innermost, ds_info.dims[2]);
				ds_info.column_type = LogicalType::ARRAY(middle, ds_info.dims[1]);
			} else {
				H5Tclose(type_id);
				H5Dclose(dataset_id);
				H5Fclose(file_id);
				throw IOException("Datasets with more than 4 dimensions are not currently supported");
			}

			H5Tclose(type_id);
			H5Dclose(dataset_id);

			result->columns.push_back(std::move(ds_info));
		}
	}

	H5Fclose(file_id);

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

	// Lock for all HDF5 operations (not thread-safe)
	std::lock_guard<std::mutex> lock(hdf5_global_mutex);

	// Open file (with error suppression)
	{
		H5ErrorSuppressor suppress;
		result->file_id = H5Fopen(bind_data.filename.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
	}

	if (result->file_id < 0) {
		throw IOException("Failed to open HDF5 file: " + bind_data.filename);
	}

	// Process each column to create its runtime state
	for (const auto &col : bind_data.columns) {
		std::visit(
		    [&](auto &&spec) {
			    using T = std::decay_t<decltype(spec)>;

			    if constexpr (std::is_same_v<T, RegularColumnSpec>) {
				    // Regular column - open dataset (with error suppression)
				    hid_t dataset_id;
				    {
					    H5ErrorSuppressor suppress;
					    dataset_id = H5Dopen2(result->file_id, spec.path.c_str(), H5P_DEFAULT);
				    }

				    if (dataset_id < 0) {
					    throw IOException("Failed to open dataset: " + spec.path);
				    }

				    // Cache the file dataspace (reused across all chunks)
				    hid_t file_space = H5Dget_space(dataset_id);
				    if (file_space < 0) {
					    H5Dclose(dataset_id);
					    throw IOException("Failed to get dataspace for dataset: " + spec.path);
				    }

				    RegularColumnState state;
				    state.dataset_id = dataset_id;
				    state.file_space = file_space;
				    result->column_states.push_back(std::move(state));

			    } else if constexpr (std::is_same_v<T, RSEColumnSpec>) {
				    // RSE column - load run_starts and values using stored types from Bind
				    RSEColumnState rse_col;

				    // Open datasets (types were inspected in Bind phase)
				    hid_t starts_ds;
				    hid_t values_ds;
				    {
					    H5ErrorSuppressor suppress;
					    starts_ds = H5Dopen2(result->file_id, spec.run_starts_path.c_str(), H5P_DEFAULT);
					    if (starts_ds < 0) {
						    throw IOException("Failed to open RSE run_starts dataset: " + spec.run_starts_path);
					    }

					    values_ds = H5Dopen2(result->file_id, spec.values_path.c_str(), H5P_DEFAULT);
					    if (values_ds < 0) {
						    H5Dclose(starts_ds);
						    throw IOException("Failed to open RSE values dataset: " + spec.values_path);
					    }
				    }

				    // Get array sizes
				    hid_t starts_space = H5Dget_space(starts_ds);
				    hssize_t num_runs_hssize = H5Sget_simple_extent_npoints(starts_space);
				    H5Sclose(starts_space);

				    hid_t values_space = H5Dget_space(values_ds);
				    hssize_t num_values_hssize = H5Sget_simple_extent_npoints(values_space);
				    H5Sclose(values_space);

				    if (num_runs_hssize < 0 || num_values_hssize < 0) {
					    H5Dclose(values_ds);
					    H5Dclose(starts_ds);
					    throw IOException("Failed to get dataset sizes for RSE column");
				    }

				    size_t num_runs = static_cast<size_t>(num_runs_hssize);
				    size_t num_values = static_cast<size_t>(num_values_hssize);

				    // Validate: run_starts and values must have same size
				    if (num_runs != num_values) {
					    H5Dclose(values_ds);
					    H5Dclose(starts_ds);
					    throw IOException("RSE run_starts and values must have same size. Got " +
					                      std::to_string(num_runs) + " and " + std::to_string(num_values));
				    }

				    // Read run_starts - validate it's an integer type, then let HDF5 convert to idx_t (uint64_t)
				    H5T_class_t starts_class = H5Tget_class(spec.run_starts_h5_type);
				    if (starts_class != H5T_INTEGER) {
					    H5Dclose(values_ds);
					    H5Dclose(starts_ds);
					    throw IOException("RSE run_starts must be integer type");
				    }

				    // Read directly into run_starts - HDF5 automatically converts from file type to uint64_t
				    rse_col.run_starts.resize(num_runs);
				    herr_t status =
				        H5Dread(starts_ds, H5T_NATIVE_UINT64, H5S_ALL, H5S_ALL, H5P_DEFAULT, rse_col.run_starts.data());

				    if (status < 0) {
					    H5Dclose(values_ds);
					    H5Dclose(starts_ds);
					    throw IOException("Failed to read run_starts from: " + spec.run_starts_path);
				    }

				    H5Dclose(starts_ds);

				    // Validate run_starts
				    if (num_runs > 0 && rse_col.run_starts[0] != 0) {
					    H5Dclose(values_ds);
					    throw IOException("RSE run_starts must begin with 0, got " +
					                      std::to_string(rse_col.run_starts[0]));
				    }
				    for (size_t i = 1; i < num_runs; i++) {
					    if (rse_col.run_starts[i] <= rse_col.run_starts[i - 1]) {
						    H5Dclose(values_ds);
						    throw IOException("RSE run_starts must be strictly increasing");
					    }
				    }
				    if (num_runs > 0 && rse_col.run_starts.back() >= bind_data.num_rows) {
					    H5Dclose(values_ds);
					    throw IOException("RSE run_starts contains index " + std::to_string(rse_col.run_starts.back()) +
					                      " which exceeds dataset length " + std::to_string(bind_data.num_rows));
				    }

				    // Read values using stored type and type dispatcher
				    // Directly populate typed vector (no Value object overhead)
				    DispatchOnDuckDBType(spec.column_type, [&](auto type_tag) {
					    using T = typename decltype(type_tag)::type;

					    if constexpr (std::is_same_v<T, string>) {
						    // String handling
						    htri_t is_variable = H5Tis_variable_str(spec.values_h5_type);
						    std::vector<string> string_values;
						    string_values.reserve(num_values);

						    if (is_variable > 0) {
							    // Variable-length strings
							    std::vector<char *> temp(num_values);
							    H5Dread(values_ds, spec.values_h5_type, H5S_ALL, H5S_ALL, H5P_DEFAULT, temp.data());
							    for (size_t i = 0; i < num_values; i++) {
								    string_values.push_back(temp[i] ? string(temp[i]) : string());
							    }
							    // Reclaim memory
							    hsize_t mem_dim = num_values;
							    hid_t mem_space = H5Screate_simple(1, &mem_dim, nullptr);
							    H5Dvlen_reclaim(spec.values_h5_type, mem_space, H5P_DEFAULT, temp.data());
							    H5Sclose(mem_space);
						    } else {
							    // Fixed-length strings
							    size_t str_len = H5Tget_size(spec.values_h5_type);
							    std::vector<char> buffer(num_values * str_len);
							    H5Dread(values_ds, spec.values_h5_type, H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer.data());
							    for (size_t i = 0; i < num_values; i++) {
								    char *str_ptr = buffer.data() + (i * str_len);
								    size_t actual_len = strnlen(str_ptr, str_len);
								    string_values.push_back(string(str_ptr, actual_len));
							    }
						    }
						    rse_col.values = std::move(string_values);
					    } else {
						    // Numeric types: read directly into typed vector
						    std::vector<T> typed_values(num_values);
						    H5Dread(values_ds, spec.values_h5_type, H5S_ALL, H5S_ALL, H5P_DEFAULT, typed_values.data());
						    rse_col.values = std::move(typed_values);
					    }
				    });

				    H5Dclose(values_ds);

				    // Initialize runtime state
				    rse_col.current_run = 0;
				    rse_col.next_run_start = (num_runs > 1) ? rse_col.run_starts[1] : bind_data.num_rows;

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

		for (const auto &[col_idx, col_filters] : filters_by_column) {
			auto &rse_spec = std::get<RSEColumnSpec>(bind_data.columns[col_idx]);
			auto &rse_state = std::get<RSEColumnState>(result->column_states[col_idx]);

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
						    bool matches = false;
						    switch (comparison) {
						    case ExpressionType::COMPARE_EQUAL:
							    matches = (value == filter_val);
							    break;
						    case ExpressionType::COMPARE_GREATERTHAN:
							    matches = (value > filter_val);
							    break;
						    case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
							    matches = (value >= filter_val);
							    break;
						    case ExpressionType::COMPARE_LESSTHAN:
							    matches = (value < filter_val);
							    break;
						    case ExpressionType::COMPARE_LESSTHANOREQUALTO:
							    matches = (value <= filter_val);
							    break;
						    default:
							    break;
						    }
						    if (!matches) {
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
// Helper function to scan an RSE column
static void ScanRSEColumn(const RSEColumnSpec &spec, RSEColumnState &state, Vector &result_vector, idx_t position,
                          idx_t to_read, hsize_t num_rows) {
	// Dispatch once per chunk, not per row
	DispatchOnDuckDBType(spec.column_type, [&](auto type_tag) {
		using T = typename decltype(type_tag)::type;

		// Access typed vector directly (no Value overhead)
		const auto &typed_values = std::get<std::vector<T>>(state.values);

		// Advance to correct run if needed (rare - only at run boundaries)
		while (position >= state.next_run_start) {
			state.current_run++;
			state.next_run_start =
			    (state.current_run + 1 < state.run_starts.size()) ? state.run_starts[state.current_run + 1] : num_rows;
		}

		// OPTIMIZATION: Check if entire chunk belongs to single run
		// With avg run length ~10k and chunk size 2048, this is true ~83% of the time!
		idx_t rows_in_current_run = state.next_run_start - position;
		if (rows_in_current_run >= to_read) {
			// Entire chunk is one value - emit CONSTANT_VECTOR (no fill loop needed!)
			result_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
			const T &run_value = typed_values[state.current_run];

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
			// State is already correctly positioned from initial while loop or previous iteration
			idx_t current_row = position + i;

			// Calculate how many rows belong to current run
			rows_in_current_run = state.next_run_start - current_row;
			idx_t rows_to_fill = std::min(rows_in_current_run, to_read - i);

			// Get the value for this run
			const T &run_value = typed_values[state.current_run];

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
				state.current_run++;
				state.next_run_start = (state.current_run + 1 < state.run_starts.size())
				                           ? state.run_starts[state.current_run + 1]
				                           : num_rows;
			}
		}
	});
}

// Helper function to scan a regular dataset column
static void ScanRegularColumn(const RegularColumnSpec &spec, const RegularColumnState &state, Vector &result_vector,
                              idx_t position, idx_t to_read) {
	hid_t dataset_id = state.dataset_id;

	// Create memory and file dataspaces for reading
	hid_t mem_space;
	hid_t file_space = state.file_space; // Use cached dataspace

	if (spec.ndims == 1) {
		// 1D dataset
		hsize_t mem_dims[1] = {to_read};
		mem_space = H5Screate_simple(1, mem_dims, nullptr);

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
		mem_space = H5Screate_simple(spec.ndims, mem_dims.data(), nullptr);

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
		// Handle string data
		// Check if variable-length or fixed-length
		htri_t is_variable = H5Tis_variable_str(spec.h5_type_id);

		if (is_variable > 0) {
			// Variable-length strings
			std::vector<char *> string_data(to_read);

			// Read using the file type directly
			herr_t status =
			    H5Dread(dataset_id, spec.h5_type_id, mem_space, file_space, H5P_DEFAULT, string_data.data());

			if (status < 0) {
				H5Sclose(mem_space);
				throw IOException("Failed to read string data from dataset: " + spec.path);
			}

			// Copy strings to DuckDB vector
			for (idx_t i = 0; i < to_read; i++) {
				if (string_data[i]) {
					FlatVector::GetData<string_t>(result_vector)[i] =
					    StringVector::AddString(result_vector, string_data[i]);
				} else {
					FlatVector::SetNull(result_vector, i, true);
				}
			}

			// Free HDF5-allocated strings
			H5Dvlen_reclaim(spec.h5_type_id, mem_space, H5P_DEFAULT, string_data.data());

		} else {
			// Fixed-length strings
			size_t str_len = H5Tget_size(spec.h5_type_id);
			std::vector<char> buffer(to_read * str_len);

			herr_t status = H5Dread(dataset_id, spec.h5_type_id, mem_space, file_space, H5P_DEFAULT, buffer.data());

			if (status < 0) {
				H5Sclose(mem_space);
				throw IOException("Failed to read string data from dataset: " + spec.path);
			}

			// Copy fixed-length strings to DuckDB vector
			for (idx_t i = 0; i < to_read; i++) {
				char *str_ptr = buffer.data() + (i * str_len);
				// Find actual string length (may be null-terminated or padded)
				size_t actual_len = strnlen(str_ptr, str_len);
				FlatVector::GetData<string_t>(result_vector)[i] =
				    StringVector::AddString(result_vector, str_ptr, actual_len);
			}
		}

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
				H5Sclose(mem_space);
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
				H5Sclose(mem_space);
				throw IOException("Failed to read data from dataset: " + spec.path);
			}
		}
	}

	// Note: file_space is cached and will be closed in destructor
	H5Sclose(mem_space);
}

static void H5ReadScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<H5ReadBindData>();
	auto &gstate = data.global_state->Cast<H5ReadGlobalState>();

	// Check if we've exhausted all row ranges
	if (gstate.current_range_idx >= gstate.valid_row_ranges.size()) {
		output.SetCardinality(0);
		return;
	}

	// Get current range
	const auto &range = gstate.valid_row_ranges[gstate.current_range_idx];

	// If we haven't started this range yet, jump to it
	if (gstate.position < range.start_row) {
		gstate.position = range.start_row;
	}

	// If we've finished this range, move to next
	if (gstate.position >= range.end_row) {
		gstate.current_range_idx++;

		if (gstate.current_range_idx >= gstate.valid_row_ranges.size()) {
			output.SetCardinality(0);
			return;
		}

		// Jump to start of next range
		const auto &next_range = gstate.valid_row_ranges[gstate.current_range_idx];
		gstate.position = next_range.start_row;
	}

	// Calculate how much to read from current range
	const auto &current_range = gstate.valid_row_ranges[gstate.current_range_idx];
	idx_t remaining_in_range = current_range.end_row - gstate.position;
	idx_t to_read = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining_in_range);

	// Lock for all HDF5 operations (not thread-safe)
	std::lock_guard<std::mutex> lock(hdf5_global_mutex);

	// Process each column (regular or RSE)
	for (size_t col_idx = 0; col_idx < bind_data.columns.size(); col_idx++) {
		auto &result_vector = output.data[col_idx];
		const auto &col_spec = bind_data.columns[col_idx];
		auto &col_state = gstate.column_states[col_idx];

		// Use variant visiting to dispatch based on column type
		std::visit(
		    [&](auto &&spec, auto &&state) {
			    using SpecT = std::decay_t<decltype(spec)>;
			    using StateT = std::decay_t<decltype(state)>;

			    if constexpr (std::is_same_v<SpecT, RSEColumnSpec> && std::is_same_v<StateT, RSEColumnState>) {
				    // RSE column - call helper function
				    ScanRSEColumn(spec, state, result_vector, gstate.position, to_read, bind_data.num_rows);

			    } else if constexpr (std::is_same_v<SpecT, RegularColumnSpec> &&
			                         std::is_same_v<StateT, RegularColumnState>) {
				    // Regular dataset - call helper function
				    ScanRegularColumn(spec, state, result_vector, gstate.position, to_read);
			    }
		    },
		    col_spec, col_state);
	}

	gstate.position += to_read;
	output.SetCardinality(to_read);
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

	// Enable predicate pushdown for RSE columns
	// We claim RSE filters for I/O optimization but keep them for post-scan verification
	// DuckDB applies all filters post-scan (defensive, ensures correctness)
	h5_read.pushdown_complex_filter = H5ReadPushdownComplexFilter;

	loader.RegisterFunction(h5_read);
}

//===--------------------------------------------------------------------===//
// h5_attributes - Read attributes from HDF5 datasets/groups
//===--------------------------------------------------------------------===//

// Helper to convert HDF5 attribute type (including arrays) to DuckDB type
static LogicalType H5AttributeTypeToDuckDBType(hid_t type_id) {
	H5T_class_t type_class = H5Tget_class(type_id);

	if (type_class == H5T_ARRAY) {
		// Get the base type of the array
		hid_t base_type = H5Tget_super(type_id);
		if (base_type < 0) {
			throw IOException("Failed to get array base type");
		}

		// Get array dimensions
		int ndims = H5Tget_array_ndims(type_id);
		if (ndims < 0) {
			H5Tclose(base_type);
			throw IOException("Failed to get array dimensions");
		}
		if (ndims != 1) {
			H5Tclose(base_type);
			throw IOException("Only 1D array attributes are supported, found " + std::to_string(ndims) + "D array");
		}

		// Get the size of the array
		hsize_t dims[1];
		if (H5Tget_array_dims2(type_id, dims) < 0) {
			H5Tclose(base_type);
			throw IOException("Failed to get array dimensions");
		}

		// Convert base type to DuckDB type
		LogicalType element_type = H5TypeToDuckDBType(base_type);
		H5Tclose(base_type);

		// Return ARRAY type with fixed size
		return LogicalType::ARRAY(element_type, dims[0]);
	}

	// For non-array types, use the existing converter
	return H5TypeToDuckDBType(type_id);
}

struct AttributeInfo {
	std::string name;
	LogicalType type;
	H5TypeHandle h5_type;
};

struct H5AttributesBindData : public TableFunctionData {
	std::string filename;
	std::string object_path;
	std::vector<AttributeInfo> attributes;
};

struct H5AttributesGlobalState : public GlobalTableFunctionState {
	bool done = false;
};

// Callback for H5Aiterate2 to collect attribute names
static herr_t attr_info_callback(hid_t location_id, const char *attr_name, const H5A_info_t *ainfo, void *op_data) {
	auto &attributes = *reinterpret_cast<std::vector<AttributeInfo> *>(op_data);

	// Open the attribute
	hid_t attr_id = H5Aopen(location_id, attr_name, H5P_DEFAULT);
	if (attr_id < 0) {
		throw IOException("Failed to open attribute: " + std::string(attr_name));
	}

	// Get the attribute's datatype
	hid_t type_id = H5Aget_type(attr_id);
	if (type_id < 0) {
		H5Aclose(attr_id);
		throw IOException("Failed to get type for attribute: " + std::string(attr_name));
	}

	// Get the dataspace to check if it's scalar or simple
	hid_t space_id = H5Aget_space(attr_id);
	if (space_id < 0) {
		H5Tclose(type_id);
		H5Aclose(attr_id);
		throw IOException("Failed to get dataspace for attribute: " + std::string(attr_name));
	}

	H5S_class_t space_class = H5Sget_simple_extent_type(space_id);

	// Check dataspace dimensions
	int ndims = H5Sget_simple_extent_ndims(space_id);
	hsize_t dims[H5S_MAX_RANK];
	if (ndims > 0) {
		H5Sget_simple_extent_dims(space_id, dims, nullptr);
	}

	H5Sclose(space_id);

	// We support:
	// 1. Scalar dataspaces (ndims == 0 or space_class == H5S_SCALAR)
	// 2. Simple 1D dataspaces for array attributes
	if (space_class != H5S_SCALAR && space_class != H5S_SIMPLE) {
		H5Tclose(type_id);
		H5Aclose(attr_id);
		throw IOException("Attribute '" + std::string(attr_name) + "' has unsupported dataspace class");
	}

	if (space_class == H5S_SIMPLE && ndims > 1) {
		H5Tclose(type_id);
		H5Aclose(attr_id);
		throw IOException("Attribute '" + std::string(attr_name) +
		                  "' has unsupported multidimensional dataspace (only 1D arrays supported)");
	}

	// Convert to DuckDB type
	LogicalType duckdb_type;
	try {
		// If the dataspace is SIMPLE (1D), create an ARRAY type
		if (space_class == H5S_SIMPLE && ndims == 1) {
			LogicalType element_type = H5TypeToDuckDBType(type_id);
			duckdb_type = LogicalType::ARRAY(element_type, dims[0]);
		} else {
			// For scalar dataspaces, use the normal converter (which handles H5T_ARRAY types)
			duckdb_type = H5AttributeTypeToDuckDBType(type_id);
		}
	} catch (const std::exception &e) {
		H5Tclose(type_id);
		H5Aclose(attr_id);
		throw IOException("Attribute '" + std::string(attr_name) + "' has unsupported type: " + std::string(e.what()));
	}

	// Store attribute info
	AttributeInfo info;
	info.name = attr_name;
	info.type = duckdb_type;
	info.h5_type = H5TypeHandle(type_id);

	attributes.push_back(std::move(info));

	H5Tclose(type_id);
	H5Aclose(attr_id);

	return 0; // Continue iteration
}

static unique_ptr<FunctionData> H5AttributesBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<H5AttributesBindData>();

	// Get parameters
	result->filename = input.inputs[0].GetValue<string>();
	result->object_path = input.inputs[1].GetValue<string>();

	// Lock for all HDF5 operations (not thread-safe)
	std::lock_guard<std::mutex> lock(hdf5_global_mutex);

	// Open the HDF5 file
	H5ErrorSuppressor suppress_errors;
	hid_t file_id = H5Fopen(result->filename.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
	if (file_id < 0) {
		throw IOException("Failed to open HDF5 file: " + result->filename);
	}

	// Open the object (dataset or group)
	hid_t obj_id = H5Oopen(file_id, result->object_path.c_str(), H5P_DEFAULT);
	if (obj_id < 0) {
		H5Fclose(file_id);
		throw IOException("Failed to open object: " + result->object_path + " in file: " + result->filename);
	}

	// Iterate through attributes
	hsize_t idx = 0;
	herr_t status = H5Aiterate2(obj_id, H5_INDEX_NAME, H5_ITER_NATIVE, &idx, attr_info_callback, &result->attributes);

	H5Oclose(obj_id);
	H5Fclose(file_id);

	if (status < 0) {
		throw IOException("Failed to iterate attributes for: " + result->object_path);
	}

	// Build the return schema - one column per attribute
	for (const auto &attr : result->attributes) {
		names.push_back(attr.name);
		return_types.push_back(attr.type);
	}

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> H5AttributesInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<H5AttributesGlobalState>();
}

static void H5AttributesScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &gstate = input.global_state->Cast<H5AttributesGlobalState>();
	auto &bind_data = input.bind_data->Cast<H5AttributesBindData>();

	// Only return one row
	if (gstate.done) {
		output.SetCardinality(0);
		return;
	}

	// Lock for all HDF5 operations (not thread-safe)
	std::lock_guard<std::mutex> lock(hdf5_global_mutex);

	// Open the file and object
	H5ErrorSuppressor suppress_errors;
	hid_t file_id = H5Fopen(bind_data.filename.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
	if (file_id < 0) {
		throw IOException("Failed to open HDF5 file: " + bind_data.filename);
	}

	hid_t obj_id = H5Oopen(file_id, bind_data.object_path.c_str(), H5P_DEFAULT);
	if (obj_id < 0) {
		H5Fclose(file_id);
		throw IOException("Failed to open object: " + bind_data.object_path);
	}

	// Read each attribute and fill the corresponding column
	for (idx_t col_idx = 0; col_idx < bind_data.attributes.size(); col_idx++) {
		const auto &attr_info = bind_data.attributes[col_idx];
		auto &result_vector = output.data[col_idx];

		// Open the attribute
		hid_t attr_id = H5Aopen(obj_id, attr_info.name.c_str(), H5P_DEFAULT);
		if (attr_id < 0) {
			H5Oclose(obj_id);
			H5Fclose(file_id);
			throw IOException("Failed to open attribute: " + attr_info.name);
		}

		// Read the attribute value based on its type
		if (attr_info.type.id() == LogicalTypeId::ARRAY) {
			// Handle array attributes
			auto array_child_type = ArrayType::GetChildType(attr_info.type);
			auto array_size = ArrayType::GetSize(attr_info.type);

			// Get the child vector where array data is stored
			auto &child_vector = ArrayVector::GetEntry(result_vector);

			// Dispatch on the element type to read the array data directly into child vector
			DispatchOnDuckDBType(array_child_type, [&](auto type_tag) {
				using T = typename decltype(type_tag)::type;

				// Get pointer to child vector data
				auto child_data = FlatVector::GetData<T>(child_vector);

				// Read the array attribute directly into the child vector
				if (H5Aread(attr_id, attr_info.h5_type.get(), child_data) < 0) {
					throw IOException("Failed to read array attribute: " + attr_info.name);
				}
			});

		} else if (attr_info.type.id() == LogicalTypeId::VARCHAR) {
			// Handle string attributes
			htri_t is_variable = H5Tis_variable_str(attr_info.h5_type.get());

			if (is_variable > 0) {
				// Variable-length string
				char *str_ptr = nullptr;
				if (H5Aread(attr_id, attr_info.h5_type.get(), &str_ptr) < 0) {
					H5Aclose(attr_id);
					H5Oclose(obj_id);
					H5Fclose(file_id);
					throw IOException("Failed to read variable-length string attribute: " + attr_info.name);
				}

				if (str_ptr) {
					FlatVector::GetData<string_t>(result_vector)[0] = StringVector::AddString(result_vector, str_ptr);
					// Free HDF5-allocated string
					free(str_ptr);
				} else {
					FlatVector::SetNull(result_vector, 0, true);
				}

			} else {
				// Fixed-length string
				size_t str_len = H5Tget_size(attr_info.h5_type.get());
				std::vector<char> buffer(str_len);

				if (H5Aread(attr_id, attr_info.h5_type.get(), buffer.data()) < 0) {
					H5Aclose(attr_id);
					H5Oclose(obj_id);
					H5Fclose(file_id);
					throw IOException("Failed to read fixed-length string attribute: " + attr_info.name);
				}

				// Find actual string length (may be null-terminated or space-padded)
				size_t actual_len = strnlen(buffer.data(), str_len);
				FlatVector::GetData<string_t>(result_vector)[0] =
				    StringVector::AddString(result_vector, buffer.data(), actual_len);
			}

		} else {
			// Handle scalar attributes - use type dispatcher
			DispatchOnDuckDBType(attr_info.type, [&](auto type_tag) {
				using T = typename decltype(type_tag)::type;

				T value;
				if (H5Aread(attr_id, attr_info.h5_type.get(), &value) < 0) {
					throw IOException("Failed to read attribute: " + attr_info.name);
				}

				// Store the value in the output vector
				auto data = FlatVector::GetData<T>(result_vector);
				data[0] = value;
			});
		}

		H5Aclose(attr_id);
	}

	H5Oclose(obj_id);
	H5Fclose(file_id);

	gstate.done = true;
	output.SetCardinality(1);
}

void RegisterH5AttributesFunction(ExtensionLoader &loader) {
	TableFunction h5_attributes("h5_attributes", {LogicalType::VARCHAR, LogicalType::VARCHAR}, H5AttributesScan,
	                            H5AttributesBind, H5AttributesInit);
	h5_attributes.name = "h5_attributes";

	loader.RegisterFunction(h5_attributes);
}

} // namespace duckdb
