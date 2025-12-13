#include "h5_functions.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/exception.hpp"
#include <vector>
#include <string>
#include <limits>

namespace duckdb {

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
        // Disable HDF5's automatic error printing
        H5E_auto2_t old_func;
        void *old_client_data;
        H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
        H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

        // Open the HDF5 file
        hid_t file_id = H5Fopen(bind_data.filename.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);

        // Restore error handler
        H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);

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

// Per-dataset information
struct DatasetInfo {
    std::string path;
    std::string column_name;
    LogicalType column_type;
    hid_t h5_type_id;
    bool is_string;
    int ndims;
    std::vector<hsize_t> dims;
    size_t element_size;
};

// Data for h5_read table function
struct H5ReadBindData : public TableFunctionData {
    std::string filename;
    std::vector<DatasetInfo> datasets;
    hsize_t num_rows;  // Minimum rows across all datasets
};

struct H5ReadGlobalState : public GlobalTableFunctionState {
    hid_t file_id;
    std::vector<hid_t> dataset_ids;
    idx_t position;

    H5ReadGlobalState() : file_id(-1), position(0) {}

    ~H5ReadGlobalState() {
        for (auto dataset_id : dataset_ids) {
            if (dataset_id >= 0) {
                H5Dclose(dataset_id);
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

    // Get parameters - first is filename, rest are dataset paths
    if (input.inputs.size() < 2) {
        throw IOException("h5_read requires at least 2 arguments: filename and dataset path(s)");
    }

    result->filename = input.inputs[0].GetValue<string>();
    size_t num_datasets = input.inputs.size() - 1;

    // Disable HDF5 error printing
    H5E_auto2_t old_func;
    void *old_client_data;
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

    // Open file once
    hid_t file_id = H5Fopen(result->filename.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);

    // Restore error handler
    H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);

    if (file_id < 0) {
        throw IOException("Failed to open HDF5 file: " + result->filename);
    }

    // Track minimum rows across all datasets
    hsize_t min_rows = std::numeric_limits<hsize_t>::max();

    // Process each dataset
    for (size_t i = 0; i < num_datasets; i++) {
        DatasetInfo ds_info;
        ds_info.path = input.inputs[i + 1].GetValue<string>();
        ds_info.column_name = GetColumnName(ds_info.path);

        // Disable HDF5 error printing for dataset opening
        H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
        H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

        // Open dataset
        hid_t dataset_id = H5Dopen2(file_id, ds_info.path.c_str(), H5P_DEFAULT);

        // Restore error handler
        H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);

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
        ds_info.h5_type_id = H5Tcopy(type_id);

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

        result->datasets.push_back(std::move(ds_info));
    }

    H5Fclose(file_id);

    // Set the minimum row count
    result->num_rows = min_rows;

    // Build output schema
    for (const auto &ds : result->datasets) {
        names.push_back(ds.column_name);
        return_types.push_back(ds.column_type);
    }

    return std::move(result);
}

// Init function - open file and dataset for reading
static unique_ptr<GlobalTableFunctionState> H5ReadInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<H5ReadBindData>();
    auto result = make_uniq<H5ReadGlobalState>();

    // Disable HDF5 error printing
    H5E_auto2_t old_func;
    void *old_client_data;
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

    // Open file
    result->file_id = H5Fopen(bind_data.filename.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);

    // Restore error handler
    H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);

    if (result->file_id < 0) {
        throw IOException("Failed to open HDF5 file: " + bind_data.filename);
    }

    // Disable HDF5 error printing for dataset opening
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

    // Open all datasets
    for (const auto &ds : bind_data.datasets) {
        hid_t dataset_id = H5Dopen2(result->file_id, ds.path.c_str(), H5P_DEFAULT);

        if (dataset_id < 0) {
            // Restore error handler before throwing
            H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);
            throw IOException("Failed to open dataset: " + ds.path);
        }

        result->dataset_ids.push_back(dataset_id);
    }

    // Restore error handler
    H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);

    result->position = 0;

    return std::move(result);
}

// Scan function - read data chunks
static void H5ReadScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &bind_data = data.bind_data->Cast<H5ReadBindData>();
    auto &gstate = data.global_state->Cast<H5ReadGlobalState>();

    idx_t remaining = bind_data.num_rows - gstate.position;
    if (remaining == 0) {
        output.SetCardinality(0);
        return;
    }

    idx_t to_read = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);

    // Read each dataset into its corresponding column
    for (size_t col_idx = 0; col_idx < bind_data.datasets.size(); col_idx++) {
        const auto &ds = bind_data.datasets[col_idx];
        hid_t dataset_id = gstate.dataset_ids[col_idx];
        auto &result_vector = output.data[col_idx];

        // Create memory and file dataspaces for reading
        hid_t mem_space;
        hid_t file_space = H5Dget_space(dataset_id);

        if (ds.ndims == 1) {
            // 1D dataset
            hsize_t mem_dims[1] = {to_read};
            mem_space = H5Screate_simple(1, mem_dims, nullptr);

            hsize_t start[1] = {gstate.position};
            hsize_t count[1] = {to_read};
            H5Sselect_hyperslab(file_space, H5S_SELECT_SET, start, nullptr, count, nullptr);
        } else {
            // Multi-dimensional dataset
            // Create memory space with same dimensionality as file
            std::vector<hsize_t> mem_dims(ds.ndims);
            mem_dims[0] = to_read;
            for (int i = 1; i < ds.ndims; i++) {
                mem_dims[i] = ds.dims[i];
            }
            mem_space = H5Screate_simple(ds.ndims, mem_dims.data(), nullptr);

            // Select hyperslab from file
            std::vector<hsize_t> start(ds.ndims, 0);
            std::vector<hsize_t> count(ds.ndims);
            start[0] = gstate.position;
            count[0] = to_read;
            for (int i = 1; i < ds.ndims; i++) {
                count[i] = ds.dims[i];
            }
            H5Sselect_hyperslab(file_space, H5S_SELECT_SET, start.data(), nullptr, count.data(), nullptr);
        }

        // Read data based on type
        if (ds.is_string) {
            // Handle string data
            // Check if variable-length or fixed-length
            htri_t is_variable = H5Tis_variable_str(ds.h5_type_id);

            if (is_variable > 0) {
                // Variable-length strings
                std::vector<char*> string_data(to_read);

                // Read using the file type directly
                herr_t status = H5Dread(dataset_id, ds.h5_type_id, mem_space, file_space,
                                        H5P_DEFAULT, string_data.data());

                if (status < 0) {
                    H5Sclose(file_space);
                    H5Sclose(mem_space);
                    throw IOException("Failed to read string data from dataset: " + ds.path);
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
                H5Dvlen_reclaim(ds.h5_type_id, mem_space, H5P_DEFAULT, string_data.data());

            } else {
                // Fixed-length strings
                size_t str_len = H5Tget_size(ds.h5_type_id);
                std::vector<char> buffer(to_read * str_len);

                herr_t status = H5Dread(dataset_id, ds.h5_type_id, mem_space,
                                        file_space, H5P_DEFAULT, buffer.data());

                if (status < 0) {
                    H5Sclose(file_space);
                    H5Sclose(mem_space);
                    throw IOException("Failed to read string data from dataset: " + ds.path);
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
            if (ds.ndims == 1) {
                // 1D dataset: read directly into DuckDB vector
                void *data_ptr = nullptr;

                // Get pointer to the correct type in the DuckDB vector
                switch (ds.column_type.id()) {
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

                herr_t status = H5Dread(dataset_id, ds.h5_type_id, mem_space,
                                        file_space, H5P_DEFAULT, data_ptr);

                if (status < 0) {
                    H5Sclose(file_space);
                    H5Sclose(mem_space);
                    throw IOException("Failed to read data from dataset: " + ds.path);
                }

            } else {
                // Multi-dimensional dataset: read into buffer, then populate arrays
                // For arrays in DuckDB, data is stored contiguously in the innermost child vector
                Vector *current_vector = &result_vector;
                LogicalType current_type = ds.column_type;

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
                herr_t status = H5Dread(dataset_id, ds.h5_type_id, mem_space,
                                        file_space, H5P_DEFAULT, child_data);

                if (status < 0) {
                    H5Sclose(file_space);
                    H5Sclose(mem_space);
                    throw IOException("Failed to read data from dataset: " + ds.path);
                }
            }
        }

        H5Sclose(file_space);
        H5Sclose(mem_space);
    }

    gstate.position += to_read;
    output.SetCardinality(to_read);
}

void RegisterH5ReadFunction(ExtensionLoader &loader) {
    // First argument is filename (VARCHAR), then 1+ dataset paths (VARCHAR)
    TableFunction h5_read("h5_read", {LogicalType::VARCHAR, LogicalType::VARCHAR},
                          H5ReadScan, H5ReadBind, H5ReadInit);
    h5_read.name = "h5_read";
    // Allow additional VARCHAR arguments for multiple datasets
    h5_read.varargs = LogicalType::VARCHAR;

    loader.RegisterFunction(h5_read);
}

} // namespace duckdb
