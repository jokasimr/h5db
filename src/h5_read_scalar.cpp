#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "h5_read_shared.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#if __has_include("duckdb/common/vector/list_vector.hpp")
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#else
#include "duckdb/common/types/vector.hpp"
#endif
#include <unordered_map>
#include <utility>
#include <vector>

namespace duckdb {

struct H5ReadScalarBindData : public FunctionData {
	bool swmr = false;
	idx_t memory_limit;

	H5ReadScalarBindData(bool swmr_p, idx_t memory_limit_p) : swmr(swmr_p), memory_limit(memory_limit_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<H5ReadScalarBindData>(swmr, memory_limit);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<H5ReadScalarBindData>();
		return swmr == other.swmr && memory_limit == other.memory_limit;
	}
};

struct H5ReadScalarMemoryEstimate {
	idx_t retained_bytes;
	idx_t peak_bytes;
};

static idx_t H5ReadScalarSaturatingAdd(idx_t left, idx_t right) {
	auto maximum = NumericLimits<idx_t>::Maximum();
	return left > maximum - right ? maximum : left + right;
}

static idx_t H5ReadScalarSaturatingMultiply(idx_t left, idx_t right) {
	if (right != 0 && left > NumericLimits<idx_t>::Maximum() / right) {
		return NumericLimits<idx_t>::Maximum();
	}
	return left * right;
}

static idx_t H5ReadScalarArrayNodeCount(const vector<hsize_t> &dims) {
	idx_t node_count = 0;
	idx_t nodes_at_depth = 1;
	for (auto dimension : dims) {
		node_count = H5ReadScalarSaturatingAdd(node_count, nodes_at_depth);
		nodes_at_depth = H5ReadScalarSaturatingMultiply(nodes_at_depth, static_cast<idx_t>(dimension));
	}
	return node_count;
}

static H5ReadScalarMemoryEstimate H5ReadScalarNumericMemoryEstimate(idx_t element_count, idx_t element_width,
                                                                    idx_t array_node_count) {
	// The retained VARIANT has a value descriptor, child reference, and payload
	// for every leaf. The temporary typed vector and VARIANT encoder selection
	// vectors add roughly the same amount again. Include a second retained-size
	// allowance because the one-row encoded vector is copied into the result.
	auto retained = H5ReadScalarSaturatingAdd(H5ReadScalarSaturatingMultiply(element_count, element_width + 16),
	                                          H5ReadScalarSaturatingMultiply(array_node_count, 32));
	auto peak = H5ReadScalarSaturatingMultiply(retained, 3);
	return {retained, peak};
}

static H5ReadScalarMemoryEstimate H5ReadScalarStringMemoryEstimate(idx_t element_count, idx_t payload_bytes,
                                                                   idx_t array_node_count) {
	// Use a deliberately wide per-element allowance for the typed string vector,
	// VARIANT descriptors, selection vectors, and capacity rounding. The peak
	// multiplier covers the source, result copy, and HDF5 string buffers.
	auto retained = H5ReadScalarSaturatingAdd(
	    H5ReadScalarSaturatingAdd(payload_bytes, H5ReadScalarSaturatingMultiply(element_count, 96)),
	    H5ReadScalarSaturatingMultiply(array_node_count, 32));
	auto peak = H5ReadScalarSaturatingMultiply(retained, 3);
	return {retained, peak};
}

static string H5ReadScalarShapeString(const vector<hsize_t> &dims) {
	string result = "[";
	for (idx_t dimension_idx = 0; dimension_idx < dims.size(); dimension_idx++) {
		if (dimension_idx > 0) {
			result += ", ";
		}
		result += std::to_string(dims[dimension_idx]);
	}
	return result + "]";
}

class H5ReadScalarMemoryBudget {
public:
	explicit H5ReadScalarMemoryBudget(idx_t limit_p) : limit(limit_p) {
	}

	void Check(const H5ReadScalarMemoryEstimate &estimate, const string &filename, const string &dataset_path,
	           const vector<hsize_t> &dims, idx_t element_count) const {
		if (limit == NumericLimits<idx_t>::Maximum()) {
			return;
		}
		// Appending the current VARIANT can resize the result's nested child
		// buffers, briefly retaining both their old and new allocations.
		auto total_peak =
		    H5ReadScalarSaturatingAdd(H5ReadScalarSaturatingMultiply(retained_bytes, 2), estimate.peak_bytes);
		if (total_peak <= limit) {
			return;
		}
		throw InvalidInputException(
		    "Scalar h5_read memory limit exceeded for dataset %s in file %s: shape %s (%llu elements) is "
		    "estimated to require %s at peak, including values already produced in this output chunk; the "
		    "h5db_scalar_read_memory_limit is %s. Use table-valued h5_read to stream large datasets, or "
		    "increase the setting explicitly",
		    dataset_path, filename, H5ReadScalarShapeString(dims), static_cast<unsigned long long>(element_count),
		    StringUtil::BytesToHumanReadableString(total_peak), StringUtil::BytesToHumanReadableString(limit));
	}

	void Charge(const H5ReadScalarMemoryEstimate &estimate, const string &filename, const string &dataset_path,
	            const vector<hsize_t> &dims, idx_t element_count) {
		if (limit == NumericLimits<idx_t>::Maximum()) {
			return;
		}
		Check(estimate, filename, dataset_path, dims, element_count);
		retained_bytes = H5ReadScalarSaturatingAdd(retained_bytes, estimate.retained_bytes);
	}

private:
	idx_t limit;
	idx_t retained_bytes = 0;
};

static vector<hsize_t> H5ReadScalarDatasetDimensions(hid_t space, const string &filename, const string &dataset_path) {
	auto ndims = H5Sget_simple_extent_ndims(space);
	if (ndims < 0) {
		throw IOException(FormatDatasetError("Failed to get dataset dimensions", filename, dataset_path));
	}

	vector<hsize_t> dims(ndims);
	if (ndims > 0 && H5Sget_simple_extent_dims(space, dims.data(), nullptr) < 0) {
		throw IOException(FormatDatasetError("Failed to get dataset dimensions", filename, dataset_path));
	}
	return dims;
}

static idx_t H5ReadScalarDatasetElementCount(hid_t space, const string &filename, const string &dataset_path) {
	auto npoints = H5Sget_simple_extent_npoints(space);
	if (npoints < 0) {
		throw IOException(FormatDatasetError("Failed to get dataset element count", filename, dataset_path));
	}
	return static_cast<idx_t>(npoints);
}

static LogicalType H5ReadScalarNestedListType(LogicalType leaf_type, idx_t rank) {
	auto result = std::move(leaf_type);
	for (idx_t dimension_idx = 0; dimension_idx < rank; dimension_idx++) {
		result = LogicalType::LIST(result);
	}
	return result;
}

static Vector &H5ReadScalarPrepareNestedListVector(Vector &root, const vector<hsize_t> &dims, const string &filename,
                                                   const string &dataset_path) {
	Vector *current = &root;
	idx_t parent_count = 1;
	for (auto h5_dimension : dims) {
		D_ASSERT(current->GetType().id() == LogicalTypeId::LIST);
		auto dimension = static_cast<idx_t>(h5_dimension);
		auto child_count = CheckedDatasetSizeProduct(parent_count, dimension, filename, dataset_path);
		ListVector::Reserve(*current, child_count);
		auto entries = ListVector::GetData(*current);
		for (idx_t parent_idx = 0; parent_idx < parent_count; parent_idx++) {
			entries[parent_idx] = list_entry_t(parent_idx * dimension, dimension);
		}
		ListVector::SetListSize(*current, child_count);
		current = &ListVector::GetEntry(*current);
		parent_count = child_count;
	}
	return *current;
}

static optional_idx H5ReadScalarKnownStringPayloadBytes(hid_t h5_type, idx_t element_count, const string &filename,
                                                        const string &dataset_path) {
	if (element_count == 0) {
		return optional_idx(0);
	}
	auto is_variable = H5Tis_variable_str(h5_type);
	if (is_variable < 0) {
		throw IOException(FormatDatasetError("Failed to inspect string type for dataset", filename, dataset_path));
	}
	if (is_variable > 0) {
		return optional_idx();
	}

	auto string_width = H5Tget_size(h5_type);
	if (string_width == 0) {
		throw IOException(FormatDatasetError("Failed to inspect string width for dataset", filename, dataset_path));
	}
	return optional_idx(H5ReadScalarSaturatingMultiply(element_count, static_cast<idx_t>(string_width)));
}

static void H5ReadScalarCastVectorToResult(Vector &source, Vector &result, idx_t result_idx, const string &filename,
                                           const string &dataset_path) {
	Vector variant(LogicalType::VARIANT(), 1);
	if (!VectorOperations::DefaultTryCast(source, variant, 1, nullptr)) {
		throw IOException(FormatDatasetError("Dataset value cannot be cast to VARIANT", filename, dataset_path));
	}
	VectorOperations::Copy(variant, result, 1, 0, result_idx);
}

static void H5ReadScalarDatasetIntoResult(ClientContext &context, hid_t file, const string &filename,
                                          const string &dataset_path, Vector &result, idx_t result_idx,
                                          H5ReadScalarMemoryBudget &memory_budget) {
	std::unique_lock<std::recursive_mutex> hdf5_lock(hdf5_global_mutex);
	auto [dataset, h5_type] = OpenDatasetAndGetType(file, filename, dataset_path);

	H5DataspaceHandle space(dataset);
	if (!space.is_valid()) {
		throw IOException(FormatDatasetError("Failed to get dataspace for dataset", filename, dataset_path));
	}
	LogicalType duckdb_type;
	try {
		duckdb_type = H5TypeToDuckDBType(h5_type);
	} catch (const IOException &error) {
		throw IOException(FormatDatasetError(ErrorData(error).RawMessage(), filename, dataset_path));
	}
	auto space_class = H5Sget_simple_extent_type(space);
	if (space_class == H5S_NO_CLASS) {
		throw IOException(FormatDatasetError("Failed to get dataset dataspace class", filename, dataset_path));
	}
	if (space_class == H5S_NULL) {
		hdf5_lock.unlock();
		result.SetValue(result_idx, Value(LogicalType::VARIANT()));
		return;
	}
	if (space_class != H5S_SCALAR && space_class != H5S_SIMPLE) {
		throw IOException(FormatDatasetError("Unsupported dataset dataspace class", filename, dataset_path));
	}

	auto dims = H5ReadScalarDatasetDimensions(space, filename, dataset_path);
	auto element_count = H5ReadScalarDatasetElementCount(space, filename, dataset_path);
	auto is_string = duckdb_type.id() == LogicalTypeId::VARCHAR;
	optional_idx known_string_payload;
	if (is_string) {
		known_string_payload = H5ReadScalarKnownStringPayloadBytes(h5_type, element_count, filename, dataset_path);
	}
	hdf5_lock.unlock();
	ThrowIfInterrupted(context);

	auto array_node_count = H5ReadScalarArrayNodeCount(dims);
	if (is_string) {
		if (known_string_payload.IsValid()) {
			memory_budget.Charge(
			    H5ReadScalarStringMemoryEstimate(element_count, known_string_payload.GetIndex(), array_node_count),
			    filename, dataset_path, dims, element_count);
		} else {
			memory_budget.Check(H5ReadScalarStringMemoryEstimate(element_count, 0, array_node_count), filename,
			                    dataset_path, dims, element_count);
		}
	} else {
		auto element_width = GetTypeIdSize(duckdb_type.InternalType());
		memory_budget.Charge(H5ReadScalarNumericMemoryEstimate(element_count, element_width, array_node_count),
		                     filename, dataset_path, dims, element_count);
	}

	auto source_type = space_class == H5S_SCALAR ? duckdb_type : H5ReadScalarNestedListType(duckdb_type, dims.size());
	Vector source(source_type, 1);
	Vector &leaf =
	    space_class == H5S_SCALAR ? source : H5ReadScalarPrepareNestedListVector(source, dims, filename, dataset_path);
	ThrowIfInterrupted(context);

	if (is_string) {
		idx_t decoded_bytes = 0;
		auto string_data = FlatVector::GetData<string_t>(leaf);
		ReadHDF5Strings(dataset, h5_type, H5S_ALL, H5S_ALL, element_count, filename, dataset_path,
		                [&](idx_t string_idx, const string &str) {
			                if (string_idx % STANDARD_VECTOR_SIZE == 0) {
				                ThrowIfInterrupted(context);
			                }
			                if (!known_string_payload.IsValid()) {
				                decoded_bytes = H5ReadScalarSaturatingAdd(decoded_bytes, str.size());
				                memory_budget.Check(
				                    H5ReadScalarStringMemoryEstimate(element_count, decoded_bytes, array_node_count),
				                    filename, dataset_path, dims, element_count);
			                }
			                string_data[string_idx] = StringVector::AddString(leaf, str);
		                });
		if (!known_string_payload.IsValid()) {
			memory_budget.Charge(H5ReadScalarStringMemoryEstimate(element_count, decoded_bytes, array_node_count),
			                     filename, dataset_path, dims, element_count);
		}
	} else {
		DispatchOnNumericType(duckdb_type, [&](auto type_tag) {
			using T = typename decltype(type_tag)::type;
			if (element_count > 0) {
				herr_t status;
				{
					std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
					H5ErrorSuppressor suppress;
					status = H5Dread(dataset, GetNativeH5Type<T>(), H5S_ALL, H5S_ALL, H5P_DEFAULT,
					                 FlatVector::GetData<T>(leaf));
				}
				if (status < 0) {
					throw IOException(FormatRemoteDatasetReadError(filename, dataset_path));
				}
			}
		});
	}
	ThrowIfInterrupted(context);
	H5ReadScalarCastVectorToResult(source, result, result_idx, filename, dataset_path);
	ThrowIfInterrupted(context);
}

class H5ReadScalarFileReader {
public:
	H5ReadScalarFileReader(ClientContext &context_p, string filename_p, bool swmr_p)
	    : context(context_p), filename(std::move(filename_p)) {
		H5ErrorSuppressor suppress;
		file = H5FileHandle(&context, filename.c_str(), H5F_ACC_RDONLY, swmr_p);
		if (!file.is_valid()) {
			throw IOException(FormatRemoteHDF5Error("Failed to open HDF5 file", filename));
		}
	}

	void ReadDataset(const string_t &path_value, Vector &result, idx_t result_idx,
	                 H5ReadScalarMemoryBudget &memory_budget) {
		ThrowIfInterrupted(context);
		auto dataset_path = path_value.GetString();
		H5ReadScalarDatasetIntoResult(context, file, filename, dataset_path, result, result_idx, memory_budget);
	}

private:
	ClientContext &context;
	string filename;
	H5FileHandle file;
};

struct H5ReadScalarFileRows {
	string filename;
	vector<idx_t> row_idxs;
};

static unique_ptr<FunctionData> H5ReadScalarBind(ClientContext &context, ScalarFunction &,
                                                 vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 2) {
		throw InvalidInputException("scalar h5_read requires exactly 2 arguments: filename and dataset path");
	}
	auto swmr = ResolveSwmrOption(context, named_parameter_map_t {});
	return make_uniq<H5ReadScalarBindData>(swmr, ResolveScalarReadMemoryLimitOption(context));
}

static void H5ReadScalarWriteFileRows(ClientContext &context, const H5ReadScalarFileRows &file_rows,
                                      const UnifiedVectorFormat &path_data, const string_t *path_ptr, Vector &result,
                                      const H5ReadScalarBindData &bind_data, H5ReadScalarMemoryBudget &memory_budget) {
	if (file_rows.row_idxs.empty()) {
		return;
	}
	ThrowIfInterrupted(context);
	H5ReadScalarFileReader reader(context, file_rows.filename, bind_data.swmr);
	for (auto row_idx : file_rows.row_idxs) {
		auto path_idx = path_data.sel->get_index(row_idx);
		reader.ReadDataset(path_ptr[path_idx], result, row_idx, memory_budget);
	}
}

static void H5ReadScalarFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<H5ReadScalarBindData>();
	if (args.size() == 0) {
		result.SetVectorType(VectorType::FLAT_VECTOR);
		return;
	}

	auto &filename_vec = args.data[0];
	auto &path_vec = args.data[1];
	UnifiedVectorFormat filename_data;
	UnifiedVectorFormat path_data;
	filename_vec.ToUnifiedFormat(args.size(), filename_data);
	path_vec.ToUnifiedFormat(args.size(), path_data);
	auto filename_ptr = UnifiedVectorFormat::GetData<string_t>(filename_data);
	auto path_ptr = UnifiedVectorFormat::GetData<string_t>(path_data);
	auto &context = state.GetContext();
	auto constant_filename = filename_vec.GetVectorType() == VectorType::CONSTANT_VECTOR;
	auto constant_path = path_vec.GetVectorType() == VectorType::CONSTANT_VECTOR;
	H5ReadScalarMemoryBudget memory_budget(bind_data.memory_limit);

	if (constant_filename && constant_path) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto filename_idx = filename_data.sel->get_index(0);
		auto path_idx = path_data.sel->get_index(0);
		if (!filename_data.validity.RowIsValid(filename_idx) || !path_data.validity.RowIsValid(path_idx)) {
			ConstantVector::SetNull(result, true);
			return;
		}

		auto filename = filename_ptr[filename_idx].GetString();
		H5ReadScalarFileReader reader(context, filename, bind_data.swmr);
		reader.ReadDataset(path_ptr[path_idx], result, 0, memory_budget);
		return;
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &validity = FlatVector::Validity(result);

	if (constant_filename) {
		auto filename_idx = filename_data.sel->get_index(0);
		if (!filename_data.validity.RowIsValid(filename_idx)) {
			for (idx_t i = 0; i < args.size(); i++) {
				validity.SetInvalid(i);
			}
			return;
		}

		H5ReadScalarFileRows file_rows;
		file_rows.filename = filename_ptr[filename_idx].GetString();
		file_rows.row_idxs.reserve(args.size());
		for (idx_t i = 0; i < args.size(); i++) {
			auto path_idx = path_data.sel->get_index(i);
			if (!path_data.validity.RowIsValid(path_idx)) {
				validity.SetInvalid(i);
				continue;
			}
			validity.SetValid(i);
			file_rows.row_idxs.push_back(i);
		}

		H5ReadScalarWriteFileRows(context, file_rows, path_data, path_ptr, result, bind_data, memory_budget);
		return;
	}

	std::unordered_map<string, idx_t> file_group_lookup;
	vector<H5ReadScalarFileRows> file_groups;
	file_group_lookup.reserve(args.size());
	file_groups.reserve(args.size());
	for (idx_t i = 0; i < args.size(); i++) {
		auto filename_idx = filename_data.sel->get_index(i);
		auto path_idx = path_data.sel->get_index(i);
		if (!filename_data.validity.RowIsValid(filename_idx) || !path_data.validity.RowIsValid(path_idx)) {
			validity.SetInvalid(i);
			continue;
		}
		validity.SetValid(i);
		auto filename = filename_ptr[filename_idx].GetString();
		auto inserted = file_group_lookup.emplace(filename, file_groups.size());
		if (inserted.second) {
			file_groups.emplace_back();
			file_groups.back().filename = std::move(filename);
		}
		file_groups[inserted.first->second].row_idxs.push_back(i);
	}

	for (const auto &file_group : file_groups) {
		H5ReadScalarWriteFileRows(context, file_group, path_data, path_ptr, result, bind_data, memory_budget);
	}
}

void RegisterH5ReadScalarFunction(ExtensionLoader &loader) {
	ScalarFunction h5_read_scalar("h5_read", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARIANT(),
	                              H5ReadScalarFunction, H5ReadScalarBind);
	h5_read_scalar.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	h5_read_scalar.SetStability(FunctionStability::CONSISTENT_WITHIN_QUERY);
	CreateScalarFunctionInfo scalar_info(std::move(h5_read_scalar));
	scalar_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	scalar_info.descriptions.push_back(
	    H5FunctionDescription({LogicalType::VARCHAR, LogicalType::VARCHAR}, {"filename", "dataset_path"},
	                          "Reads one HDF5 dataset as a VARIANT.", {"SELECT h5_read('data.h5', '/entry/dataset')"}));
	loader.RegisterFunction(std::move(scalar_info));
}

} // namespace duckdb
