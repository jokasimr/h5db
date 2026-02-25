#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "h5_raii.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/exception.hpp"
#include <vector>
#include <string>

namespace duckdb {

struct H5ObjectInfo {
	std::string path;
	std::string type;           // "group" or "dataset"
	std::string dtype;          // data type (for datasets)
	std::vector<hsize_t> shape; // shape (for datasets)
};

struct H5TreeBindData : public TableFunctionData {
	std::string filename;
	mutable std::vector<H5ObjectInfo> objects;
	mutable bool scanned = false;
};

struct H5TreeGlobalState : public GlobalTableFunctionState {
	idx_t position = 0;
};

static void WriteShapeListRow(Vector &shape_vector, idx_t row_idx, const H5ObjectInfo &obj, idx_t &shape_offset,
                              uint64_t *shape_data) {
	auto shape_entries = ListVector::GetData(shape_vector);
	auto &shape_validity = FlatVector::Validity(shape_vector);

	if (obj.type == "group") {
		shape_validity.SetInvalid(row_idx);
		shape_entries[row_idx].offset = 0;
		shape_entries[row_idx].length = 0;
		return;
	}

	shape_validity.SetValid(row_idx);
	shape_entries[row_idx].offset = shape_offset;
	shape_entries[row_idx].length = obj.shape.size();
	for (auto dim : obj.shape) {
		shape_data[shape_offset++] = static_cast<uint64_t>(dim);
	}
}

static herr_t visit_callback(hid_t obj_id, const char *name, const H5O_info_t *info, void *op_data) {
	auto &objects = *reinterpret_cast<std::vector<H5ObjectInfo> *>(op_data);

	H5ObjectInfo obj_info;
	obj_info.path = std::string("/") + name;

	if (info->type == H5O_TYPE_GROUP) {
		obj_info.type = "group";
		obj_info.dtype = "";
		obj_info.shape = {};
	} else if (info->type == H5O_TYPE_DATASET) {
		obj_info.type = "dataset";

		H5DatasetHandle dataset(obj_id, name);
		if (dataset.is_valid()) {
			hid_t type_id = H5Dget_type(dataset);
			if (type_id >= 0) {
				H5TypeHandle type = H5TypeHandle::TakeOwnershipOf(type_id);
				obj_info.dtype = H5TypeToString(type);
			}

			obj_info.shape = H5GetShape(dataset);
		}
	}

	objects.push_back(obj_info);
	return 0; // Continue iteration
}

static unique_ptr<FunctionData> H5TreeBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<H5TreeBindData>();

	result->filename = input.inputs[0].GetValue<string>();

	names = {"path", "type", "dtype", "shape"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::LIST(LogicalType::UBIGINT)};

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> H5TreeInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<H5TreeBindData>();
	auto result = make_uniq<H5TreeGlobalState>();

	if (!bind_data.scanned) {
		std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);

		H5FileHandle file;
		{
			H5ErrorSuppressor suppress;
			file = H5FileHandle(bind_data.filename.c_str(), H5F_ACC_RDONLY);
		}

		if (!file.is_valid()) {
			throw IOException("Failed to open HDF5 file: " + bind_data.filename);
		}

		H5O_info_t obj_info;
		if (H5Oget_info(file, &obj_info, H5O_INFO_BASIC) >= 0) {
			H5Ovisit(file, H5_INDEX_NAME, H5_ITER_NATIVE, visit_callback, &bind_data.objects, H5O_INFO_BASIC);
		}

		bind_data.scanned = true;
	}

	return std::move(result);
}

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

	auto &path_vector = output.data[0];
	auto &type_vector = output.data[1];
	auto &dtype_vector = output.data[2];
	auto &shape_vector = output.data[3];
	auto &shape_child = ListVector::GetEntry(shape_vector);
	idx_t total_shape_elems = 0;
	for (idx_t i = 0; i < to_process; i++) {
		const auto &obj = bind_data.objects[gstate.position + i];
		if (obj.type != "group") {
			total_shape_elems += obj.shape.size();
		}
	}
	ListVector::Reserve(shape_vector, total_shape_elems);
	auto shape_data = FlatVector::GetData<uint64_t>(shape_child);
	idx_t shape_offset = 0;

	for (idx_t i = 0; i < to_process; i++) {
		const auto &obj = bind_data.objects[gstate.position + i];

		FlatVector::GetData<string_t>(path_vector)[i] = StringVector::AddString(path_vector, obj.path);
		FlatVector::GetData<string_t>(type_vector)[i] = StringVector::AddString(type_vector, obj.type);
		FlatVector::GetData<string_t>(dtype_vector)[i] = StringVector::AddString(dtype_vector, obj.dtype);
		WriteShapeListRow(shape_vector, i, obj, shape_offset, shape_data);

		count++;
	}
	ListVector::SetListSize(shape_vector, shape_offset);

	gstate.position += count;
	output.SetCardinality(count);
}

void RegisterH5TreeFunction(ExtensionLoader &loader) {
	TableFunction h5_tree("h5_tree", {LogicalType::VARCHAR}, H5TreeScan, H5TreeBind, H5TreeInit);
	h5_tree.name = "h5_tree";

	loader.RegisterFunction(h5_tree);
}

} // namespace duckdb
