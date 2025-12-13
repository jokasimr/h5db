#include "h5_functions.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/exception.hpp"
#include <vector>
#include <string>
#include <limits>
#include <variant>

namespace duckdb {

// RAII wrapper for HDF5 error suppression
// Automatically disables HDF5 error printing on construction and restores it on destruction
class H5ErrorSuppressor {
    H5E_auto2_t old_func;
    void* old_client_data;

public:
    H5ErrorSuppressor() {
        H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
        H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);
    }

    ~H5ErrorSuppressor() {
        H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);
    }

    // Disable copy and move to prevent double restoration
    H5ErrorSuppressor(const H5ErrorSuppressor&) = delete;
    H5ErrorSuppressor& operator=(const H5ErrorSuppressor&) = delete;
    H5ErrorSuppressor(H5ErrorSuppressor&&) = delete;
    H5ErrorSuppressor& operator=(H5ErrorSuppressor&&) = delete;
};

// RAII wrapper for HDF5 type handles
// Automatically closes type handle on destruction
class H5TypeHandle {
    hid_t id;

public:
    H5TypeHandle() : id(-1) {}

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
    operator hid_t() const { return id; }
    hid_t get() const { return id; }

    // Disable copy but enable move
    H5TypeHandle(const H5TypeHandle&) = delete;
    H5TypeHandle& operator=(const H5TypeHandle&) = delete;

    H5TypeHandle(H5TypeHandle&& other) noexcept : id(other.id) {
        other.id = -1;
    }

    H5TypeHandle& operator=(H5TypeHandle&& other) noexcept {
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
                    case 1: return "uint8";
                    case 2: return "uint16";
                    case 4: return "uint32";
                    case 8: return "uint64";
                    default: return "uint" + std::to_string(size * 8);
                }
            } else {
                // Signed integer
                switch (size) {
                    case 1: return "int8";
                    case 2: return "int16";
                    case 4: return "int32";
                    case 8: return "int64";
                    default: return "int" + std::to_string(size * 8);
                }
            }
        case H5T_FLOAT:
            switch (size) {
                case 2: return "float16";
                case 4: return "float32";
                case 8: return "float64";
                default: return "float" + std::to_string(size * 8);
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
        if (i > 0) result += ", ";
        result += std::to_string(dims[i]);
    }
    result += ")";

    return result;
}

// Structure to hold information about an HDF5 object
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
    auto &objects = *reinterpret_cast<std::vector<H5ObjectInfo>*>(op_data);

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
    return 0;  // Continue iteration
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

        FlatVector::GetData<string_t>(*children[0])[i] =
            StringVector::AddString(*children[0], "rse");
        FlatVector::GetData<string_t>(*children[1])[i] =
            StringVector::AddString(*children[1], run_starts_ptr[run_starts_idx]);
        FlatVector::GetData<string_t>(*children[2])[i] =
            StringVector::AddString(*children[2], values_ptr[values_idx]);
    }

    result.SetVectorType(VectorType::FLAT_VECTOR);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void RegisterH5RseFunction(ExtensionLoader &loader) {
    child_list_t<LogicalType> struct_children = {
        {"encoding", LogicalType::VARCHAR},
        {"run_starts", LogicalType::VARCHAR},
        {"values", LogicalType::VARCHAR}
    };

    auto h5_rse = ScalarFunction(
        "h5_rse",
        {LogicalType::VARCHAR, LogicalType::VARCHAR},
        LogicalType::STRUCT(struct_children),
        H5RseFunction
    );
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
                    case 1: return LogicalType::UTINYINT;
                    case 2: return LogicalType::USMALLINT;
                    case 4: return LogicalType::UINTEGER;
                    case 8: return LogicalType::UBIGINT;
                    default: throw IOException("Unsupported unsigned integer size: " + std::to_string(size));
                }
            } else {
                // Signed integer
                switch (size) {
                    case 1: return LogicalType::TINYINT;
                    case 2: return LogicalType::SMALLINT;
                    case 4: return LogicalType::INTEGER;
                    case 8: return LogicalType::BIGINT;
                    default: throw IOException("Unsupported signed integer size: " + std::to_string(size));
                }
            }
        }
        case H5T_FLOAT:
            switch (size) {
                case 4: return LogicalType::FLOAT;
                case 8: return LogicalType::DOUBLE;
                default: throw IOException("Unsupported float size: " + std::to_string(size));
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
    H5TypeHandle h5_type_id;  // RAII wrapper - automatically closes on destruction
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
};

// A column can be either regular or RSE
using ColumnSpec = std::variant<RegularColumnSpec, RSEColumnSpec>;

// Regular column runtime state
struct RegularColumnState {
    hid_t dataset_id;
};

// RSE column runtime state
struct RSEColumnState {
    std::vector<idx_t> run_starts;
    std::vector<Value> values;
    idx_t current_run;
    idx_t next_run_start;
};

// Runtime state for a column (either regular or RSE)
using ColumnState = std::variant<RegularColumnState, RSEColumnState>;

// Data for h5_read table function
struct H5ReadBindData : public TableFunctionData {
    std::string filename;
    std::vector<ColumnSpec> columns;  // Unified column specifications
    hsize_t num_rows;  // Row count from regular datasets
};

struct H5ReadGlobalState : public GlobalTableFunctionState {
    hid_t file_id;
    std::vector<ColumnState> column_states;  // Unified column states
    idx_t position;

    H5ReadGlobalState() : file_id(-1), position(0) {}

    ~H5ReadGlobalState() {
        // Close all regular column datasets
        for (auto& state : column_states) {
            if (auto* reg_state = std::get_if<RegularColumnState>(&state)) {
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
            ds_info.h5_type_id = H5TypeHandle(type_id);  // RAII wrapper - copies and auto-closes

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
        std::visit([&](auto&& spec) {
            names.push_back(spec.column_name);
            return_types.push_back(spec.column_type);
        }, col);
    }

    return std::move(result);
}

// Init function - open file and dataset for reading
static unique_ptr<GlobalTableFunctionState> H5ReadInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<H5ReadBindData>();
    auto result = make_uniq<H5ReadGlobalState>();

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
        std::visit([&](auto&& spec) {
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

                RegularColumnState state;
                state.dataset_id = dataset_id;
                result->column_states.push_back(std::move(state));

            } else if constexpr (std::is_same_v<T, RSEColumnSpec>) {
                // RSE column - load run_starts and values
                RSEColumnState rse_col;

                // Open run_starts and values datasets (with error suppression)
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

                // Get run_starts size
                hid_t starts_space = H5Dget_space(starts_ds);
                hssize_t num_runs_hssize = H5Sget_simple_extent_npoints(starts_space);
                H5Sclose(starts_space);

                if (num_runs_hssize < 0) {
                    H5Dclose(values_ds);
                    H5Dclose(starts_ds);
                    throw IOException("Failed to get run_starts size for: " + spec.run_starts_path);
                }
                size_t num_runs = static_cast<size_t>(num_runs_hssize);

                // Get values size
                hid_t values_space = H5Dget_space(values_ds);
                hssize_t num_values_hssize = H5Sget_simple_extent_npoints(values_space);
                H5Sclose(values_space);

                if (num_values_hssize < 0) {
                    H5Dclose(values_ds);
                    H5Dclose(starts_ds);
                    throw IOException("Failed to get values size for: " + spec.values_path);
                }
                size_t num_values = static_cast<size_t>(num_values_hssize);

                // Validate: run_starts and values must have same size
                if (num_runs != num_values) {
                    H5Dclose(values_ds);
                    H5Dclose(starts_ds);
                    throw IOException("RSE run_starts and values must have same size. Got " +
                                    std::to_string(num_runs) + " and " + std::to_string(num_values));
                }

                // Read run_starts - handle different integer types
                rse_col.run_starts.resize(num_runs);
                hid_t starts_type = H5Dget_type(starts_ds);
                H5T_class_t starts_class = H5Tget_class(starts_type);
                size_t starts_size = H5Tget_size(starts_type);

                if (starts_class != H5T_INTEGER) {
                    H5Tclose(starts_type);
                    H5Dclose(values_ds);
                    H5Dclose(starts_ds);
                    throw IOException("RSE run_starts must be integer type");
                }

                H5T_sign_t starts_sign = H5Tget_sign(starts_type);

                // Read run_starts based on type
                if (starts_sign == H5T_SGN_2) {  // Signed
                    if (starts_size == 4) {
                        std::vector<int32_t> temp(num_runs);
                        H5Dread(starts_ds, H5T_NATIVE_INT32, H5S_ALL, H5S_ALL, H5P_DEFAULT, temp.data());
                        for (size_t i = 0; i < num_runs; i++) {
                            rse_col.run_starts[i] = static_cast<idx_t>(temp[i]);
                        }
                    } else if (starts_size == 8) {
                        std::vector<int64_t> temp(num_runs);
                        H5Dread(starts_ds, H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT, temp.data());
                        for (size_t i = 0; i < num_runs; i++) {
                            rse_col.run_starts[i] = static_cast<idx_t>(temp[i]);
                        }
                    } else {
                        H5Tclose(starts_type);
                        H5Dclose(values_ds);
                        H5Dclose(starts_ds);
                        throw IOException("Unsupported signed integer size for run_starts: " + std::to_string(starts_size));
                    }
                } else {  // Unsigned
                    if (starts_size == 4) {
                        std::vector<uint32_t> temp(num_runs);
                        H5Dread(starts_ds, H5T_NATIVE_UINT32, H5S_ALL, H5S_ALL, H5P_DEFAULT, temp.data());
                        for (size_t i = 0; i < num_runs; i++) {
                            rse_col.run_starts[i] = static_cast<idx_t>(temp[i]);
                        }
                    } else if (starts_size == 8) {
                        std::vector<uint64_t> temp(num_runs);
                        H5Dread(starts_ds, H5T_NATIVE_UINT64, H5S_ALL, H5S_ALL, H5P_DEFAULT, temp.data());
                        for (size_t i = 0; i < num_runs; i++) {
                            rse_col.run_starts[i] = static_cast<idx_t>(temp[i]);
                        }
                    } else {
                        H5Tclose(starts_type);
                        H5Dclose(values_ds);
                        H5Dclose(starts_ds);
                        throw IOException("Unsupported unsigned integer size for run_starts: " + std::to_string(starts_size));
                    }
                }

                H5Tclose(starts_type);
                H5Dclose(starts_ds);

                // Validate run_starts
                if (num_runs > 0 && rse_col.run_starts[0] != 0) {
                    H5Dclose(values_ds);
                    throw IOException("RSE run_starts must begin with 0, got " + std::to_string(rse_col.run_starts[0]));
                }
                for (size_t i = 1; i < num_runs; i++) {
                    if (rse_col.run_starts[i] <= rse_col.run_starts[i-1]) {
                        H5Dclose(values_ds);
                        throw IOException("RSE run_starts must be strictly increasing");
                    }
                }
                // Validate that run_starts don't exceed dataset bounds
                if (num_runs > 0 && rse_col.run_starts.back() >= bind_data.num_rows) {
                    H5Dclose(values_ds);
                    throw IOException("RSE run_starts contains index " + std::to_string(rse_col.run_starts.back()) +
                                     " which exceeds dataset length " + std::to_string(bind_data.num_rows));
                }

                // Read values - handle different types
                rse_col.values.reserve(num_values);
                hid_t values_type = H5Dget_type(values_ds);
                H5T_class_t values_class = H5Tget_class(values_type);
                size_t values_size = H5Tget_size(values_type);

                if (values_class == H5T_INTEGER) {
                    H5T_sign_t values_sign = H5Tget_sign(values_type);

                    if (values_sign == H5T_SGN_2) {  // Signed
                        if (values_size == 1) {
                            std::vector<int8_t> temp(num_values);
                            H5Dread(values_ds, H5T_NATIVE_INT8, H5S_ALL, H5S_ALL, H5P_DEFAULT, temp.data());
                            for (auto v : temp) rse_col.values.push_back(Value::TINYINT(v));
                        } else if (values_size == 2) {
                            std::vector<int16_t> temp(num_values);
                            H5Dread(values_ds, H5T_NATIVE_INT16, H5S_ALL, H5S_ALL, H5P_DEFAULT, temp.data());
                            for (auto v : temp) rse_col.values.push_back(Value::SMALLINT(v));
                        } else if (values_size == 4) {
                            std::vector<int32_t> temp(num_values);
                            H5Dread(values_ds, H5T_NATIVE_INT32, H5S_ALL, H5S_ALL, H5P_DEFAULT, temp.data());
                            for (auto v : temp) rse_col.values.push_back(Value::INTEGER(v));
                        } else if (values_size == 8) {
                            std::vector<int64_t> temp(num_values);
                            H5Dread(values_ds, H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT, temp.data());
                            for (auto v : temp) rse_col.values.push_back(Value::BIGINT(v));
                        } else {
                            H5Tclose(values_type);
                            H5Dclose(values_ds);
                            throw IOException("Unsupported signed integer size for values: " + std::to_string(values_size));
                        }
                    } else {  // Unsigned
                        if (values_size == 1) {
                            std::vector<uint8_t> temp(num_values);
                            H5Dread(values_ds, H5T_NATIVE_UINT8, H5S_ALL, H5S_ALL, H5P_DEFAULT, temp.data());
                            for (auto v : temp) rse_col.values.push_back(Value::UTINYINT(v));
                        } else if (values_size == 2) {
                            std::vector<uint16_t> temp(num_values);
                            H5Dread(values_ds, H5T_NATIVE_UINT16, H5S_ALL, H5S_ALL, H5P_DEFAULT, temp.data());
                            for (auto v : temp) rse_col.values.push_back(Value::USMALLINT(v));
                        } else if (values_size == 4) {
                            std::vector<uint32_t> temp(num_values);
                            H5Dread(values_ds, H5T_NATIVE_UINT32, H5S_ALL, H5S_ALL, H5P_DEFAULT, temp.data());
                            for (auto v : temp) rse_col.values.push_back(Value::UINTEGER(v));
                        } else if (values_size == 8) {
                            std::vector<uint64_t> temp(num_values);
                            H5Dread(values_ds, H5T_NATIVE_UINT64, H5S_ALL, H5S_ALL, H5P_DEFAULT, temp.data());
                            for (auto v : temp) rse_col.values.push_back(Value::UBIGINT(v));
                        } else {
                            H5Tclose(values_type);
                            H5Dclose(values_ds);
                            throw IOException("Unsupported unsigned integer size for values: " + std::to_string(values_size));
                        }
                    }
                } else if (values_class == H5T_FLOAT) {
                    if (values_size == 4) {
                        std::vector<float> temp(num_values);
                        H5Dread(values_ds, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, temp.data());
                        for (auto v : temp) rse_col.values.push_back(Value::FLOAT(v));
                    } else if (values_size == 8) {
                        std::vector<double> temp(num_values);
                        H5Dread(values_ds, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, temp.data());
                        for (auto v : temp) rse_col.values.push_back(Value::DOUBLE(v));
                    } else {
                        H5Tclose(values_type);
                        H5Dclose(values_ds);
                        throw IOException("Unsupported float size for values: " + std::to_string(values_size));
                    }
                } else if (values_class == H5T_STRING) {
                    htri_t is_variable = H5Tis_variable_str(values_type);

                    if (is_variable > 0) {
                        // Variable-length strings
                        std::vector<char*> temp(num_values);
                        H5Dread(values_ds, values_type, H5S_ALL, H5S_ALL, H5P_DEFAULT, temp.data());
                        for (size_t i = 0; i < num_values; i++) {
                            rse_col.values.push_back(Value(temp[i] ? string(temp[i]) : string()));
                        }
                        // Create memory space for reclamation
                        hsize_t mem_dim = num_values;
                        hid_t mem_space = H5Screate_simple(1, &mem_dim, nullptr);
                        H5Dvlen_reclaim(values_type, mem_space, H5P_DEFAULT, temp.data());
                        H5Sclose(mem_space);
                    } else {
                        // Fixed-length strings
                        size_t str_len = H5Tget_size(values_type);
                        std::vector<char> buffer(num_values * str_len);
                        H5Dread(values_ds, values_type, H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer.data());
                        for (size_t i = 0; i < num_values; i++) {
                            char *str_ptr = buffer.data() + (i * str_len);
                            size_t actual_len = strnlen(str_ptr, str_len);
                            rse_col.values.push_back(Value(string(str_ptr, actual_len)));
                        }
                    }
                } else {
                    H5Tclose(values_type);
                    H5Dclose(values_ds);
                    throw IOException("Unsupported type class for RSE values: " + std::to_string(values_class));
                }

                H5Tclose(values_type);
                H5Dclose(values_ds);

                // Initialize runtime state
                rse_col.current_run = 0;
                rse_col.next_run_start = (num_runs > 1) ? rse_col.run_starts[1] : bind_data.num_rows;

                result->column_states.push_back(std::move(rse_col));
            }
        }, col);
    }

    result->position = 0;

    return std::move(result);
}

// Scan function - read data chunks
// Helper function to scan an RSE column
static void ScanRSEColumn(const RSEColumnSpec &spec, RSEColumnState &state, Vector &result_vector,
                          idx_t position, idx_t to_read, hsize_t num_rows) {
    // Single-pass expansion
    for (idx_t i = 0; i < to_read; i++) {
        idx_t row = position + i;

        // Advance run if needed
        while (row >= state.next_run_start) {
            state.current_run++;
            state.next_run_start = (state.current_run + 1 < state.run_starts.size())
                                 ? state.run_starts[state.current_run + 1]
                                 : num_rows;
        }

        // Emit current run's value based on type
        auto &current_value = state.values[state.current_run];

        switch (spec.column_type.id()) {
            case LogicalTypeId::TINYINT:
                FlatVector::GetData<int8_t>(result_vector)[i] = current_value.template GetValue<int8_t>();
                break;
            case LogicalTypeId::SMALLINT:
                FlatVector::GetData<int16_t>(result_vector)[i] = current_value.template GetValue<int16_t>();
                break;
            case LogicalTypeId::INTEGER:
                FlatVector::GetData<int32_t>(result_vector)[i] = current_value.template GetValue<int32_t>();
                break;
            case LogicalTypeId::BIGINT:
                FlatVector::GetData<int64_t>(result_vector)[i] = current_value.template GetValue<int64_t>();
                break;
            case LogicalTypeId::UTINYINT:
                FlatVector::GetData<uint8_t>(result_vector)[i] = current_value.template GetValue<uint8_t>();
                break;
            case LogicalTypeId::USMALLINT:
                FlatVector::GetData<uint16_t>(result_vector)[i] = current_value.template GetValue<uint16_t>();
                break;
            case LogicalTypeId::UINTEGER:
                FlatVector::GetData<uint32_t>(result_vector)[i] = current_value.template GetValue<uint32_t>();
                break;
            case LogicalTypeId::UBIGINT:
                FlatVector::GetData<uint64_t>(result_vector)[i] = current_value.template GetValue<uint64_t>();
                break;
            case LogicalTypeId::FLOAT:
                FlatVector::GetData<float>(result_vector)[i] = current_value.template GetValue<float>();
                break;
            case LogicalTypeId::DOUBLE:
                FlatVector::GetData<double>(result_vector)[i] = current_value.template GetValue<double>();
                break;
            case LogicalTypeId::VARCHAR: {
                string str = current_value.template GetValue<string>();
                FlatVector::GetData<string_t>(result_vector)[i] =
                    StringVector::AddString(result_vector, str);
                break;
            }
            default:
                throw IOException("Unsupported RSE value type");
        }
    }
}

// Helper function to scan a regular dataset column
static void ScanRegularColumn(const RegularColumnSpec &spec, const RegularColumnState &state,
                               Vector &result_vector, idx_t position, idx_t to_read) {
    hid_t dataset_id = state.dataset_id;

    // Create memory and file dataspaces for reading
    hid_t mem_space;
    hid_t file_space = H5Dget_space(dataset_id);

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
            std::vector<char*> string_data(to_read);

            // Read using the file type directly
            herr_t status = H5Dread(dataset_id, spec.h5_type_id, mem_space, file_space,
                                    H5P_DEFAULT, string_data.data());

            if (status < 0) {
                H5Sclose(file_space);
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

            herr_t status = H5Dread(dataset_id, spec.h5_type_id, mem_space,
                                    file_space, H5P_DEFAULT, buffer.data());

            if (status < 0) {
                H5Sclose(file_space);
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
            void *data_ptr = nullptr;

            // Get pointer to the correct type in the DuckDB vector
            switch (spec.column_type.id()) {
                case LogicalTypeId::TINYINT:
                    data_ptr = FlatVector::GetData<int8_t>(result_vector);
                    break;
                case LogicalTypeId::SMALLINT:
                    data_ptr = FlatVector::GetData<int16_t>(result_vector);
                    break;
                case LogicalTypeId::INTEGER:
                    data_ptr = FlatVector::GetData<int32_t>(result_vector);
                    break;
                case LogicalTypeId::BIGINT:
                    data_ptr = FlatVector::GetData<int64_t>(result_vector);
                    break;
                case LogicalTypeId::UTINYINT:
                    data_ptr = FlatVector::GetData<uint8_t>(result_vector);
                    break;
                case LogicalTypeId::USMALLINT:
                    data_ptr = FlatVector::GetData<uint16_t>(result_vector);
                    break;
                case LogicalTypeId::UINTEGER:
                    data_ptr = FlatVector::GetData<uint32_t>(result_vector);
                    break;
                case LogicalTypeId::UBIGINT:
                    data_ptr = FlatVector::GetData<uint64_t>(result_vector);
                    break;
                case LogicalTypeId::FLOAT:
                    data_ptr = FlatVector::GetData<float>(result_vector);
                    break;
                case LogicalTypeId::DOUBLE:
                    data_ptr = FlatVector::GetData<double>(result_vector);
                    break;
                default:
                    H5Sclose(file_space);
                    H5Sclose(mem_space);
                    throw IOException("Unsupported data type for reading");
            }

            herr_t status = H5Dread(dataset_id, spec.h5_type_id, mem_space,
                                    file_space, H5P_DEFAULT, data_ptr);

            if (status < 0) {
                H5Sclose(file_space);
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

            // Get pointer to the innermost child data based on base type
            void *child_data = nullptr;

            switch (current_type.id()) {
                case LogicalTypeId::TINYINT:
                    child_data = FlatVector::GetData<int8_t>(*current_vector);
                    break;
                case LogicalTypeId::SMALLINT:
                    child_data = FlatVector::GetData<int16_t>(*current_vector);
                    break;
                case LogicalTypeId::INTEGER:
                    child_data = FlatVector::GetData<int32_t>(*current_vector);
                    break;
                case LogicalTypeId::BIGINT:
                    child_data = FlatVector::GetData<int64_t>(*current_vector);
                    break;
                case LogicalTypeId::UTINYINT:
                    child_data = FlatVector::GetData<uint8_t>(*current_vector);
                    break;
                case LogicalTypeId::USMALLINT:
                    child_data = FlatVector::GetData<uint16_t>(*current_vector);
                    break;
                case LogicalTypeId::UINTEGER:
                    child_data = FlatVector::GetData<uint32_t>(*current_vector);
                    break;
                case LogicalTypeId::UBIGINT:
                    child_data = FlatVector::GetData<uint64_t>(*current_vector);
                    break;
                case LogicalTypeId::FLOAT:
                    child_data = FlatVector::GetData<float>(*current_vector);
                    break;
                case LogicalTypeId::DOUBLE:
                    child_data = FlatVector::GetData<double>(*current_vector);
                    break;
                default:
                    H5Sclose(file_space);
                    H5Sclose(mem_space);
                    throw IOException("Unsupported array element type");
            }

            // Read data directly into the array child vector
            herr_t status = H5Dread(dataset_id, spec.h5_type_id, mem_space,
                                    file_space, H5P_DEFAULT, child_data);

            if (status < 0) {
                H5Sclose(file_space);
                H5Sclose(mem_space);
                throw IOException("Failed to read data from dataset: " + spec.path);
            }
        }
    }

    H5Sclose(file_space);
    H5Sclose(mem_space);
}

static void H5ReadScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &bind_data = data.bind_data->Cast<H5ReadBindData>();
    auto &gstate = data.global_state->Cast<H5ReadGlobalState>();

    idx_t remaining = bind_data.num_rows - gstate.position;
    if (remaining == 0) {
        output.SetCardinality(0);
        return;
    }

    idx_t to_read = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);

    // Process each column (regular or RSE)
    for (size_t col_idx = 0; col_idx < bind_data.columns.size(); col_idx++) {
        auto &result_vector = output.data[col_idx];
        const auto &col_spec = bind_data.columns[col_idx];
        auto &col_state = gstate.column_states[col_idx];

        // Use variant visiting to dispatch based on column type
        std::visit([&](auto&& spec, auto&& state) {
            using SpecT = std::decay_t<decltype(spec)>;
            using StateT = std::decay_t<decltype(state)>;

            if constexpr (std::is_same_v<SpecT, RSEColumnSpec> && std::is_same_v<StateT, RSEColumnState>) {
                // RSE column - call helper function
                ScanRSEColumn(spec, state, result_vector, gstate.position, to_read, bind_data.num_rows);

            } else if constexpr (std::is_same_v<SpecT, RegularColumnSpec> && std::is_same_v<StateT, RegularColumnState>) {
                // Regular dataset - call helper function
                ScanRegularColumn(spec, state, result_vector, gstate.position, to_read);

            }
        }, col_spec, col_state);
    }

    gstate.position += to_read;
    output.SetCardinality(to_read);
}

void RegisterH5ReadFunction(ExtensionLoader &loader) {
    // First argument is filename (VARCHAR), then 1+ dataset paths (VARCHAR or STRUCT for RSE)
    TableFunction h5_read("h5_read", {LogicalType::VARCHAR, LogicalType::ANY},
                          H5ReadScan, H5ReadBind, H5ReadInit);
    h5_read.name = "h5_read";
    // Allow additional ANY arguments for multiple datasets (VARCHAR or STRUCT from h5_rse())
    h5_read.varargs = LogicalType::ANY;

    loader.RegisterFunction(h5_read);
}

} // namespace duckdb
