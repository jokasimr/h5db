#include "h5_read_shared.hpp"
#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "duckdb/common/exception.hpp"
#include <vector>

namespace duckdb {

string FormatRemoteHDF5Error(const string &prefix, const string &filename) {
	return FormatRemoteFileError(prefix, filename);
}

string FormatDatasetError(const string &prefix, const string &filename, const string &dataset_path) {
	return AppendRemoteError(prefix + ": " + dataset_path + " in file: " + filename, filename);
}

string FormatRemoteDatasetReadError(const string &filename, const string &dataset_path) {
	return FormatDatasetError("Failed to read data from dataset", filename, dataset_path);
}

static string FormatInvalidDatasetStringError(const string &filename, const string &dataset_path) {
	return FormatDatasetError("Invalid unicode (byte sequence mismatch) detected in dataset", filename, dataset_path);
}

static string ValidateHDF5StringValue(string value, H5T_cset_t cset, const string &filename,
                                      const string &dataset_path) {
	if (!H5StringMatchesCharset(value, cset)) {
		throw IOException(FormatInvalidDatasetStringError(filename, dataset_path));
	}
	return value;
}

idx_t CheckedDatasetSizeProduct(idx_t left, idx_t right, const string &filename, const string &dataset_path) {
	if (right != 0 && left > NumericLimits<idx_t>::Maximum() / right) {
		throw IOException(
		    FormatDatasetError("Dataset dimensions exceed the supported in-memory size", filename, dataset_path));
	}
	return left * right;
}

std::pair<H5DatasetHandle, H5TypeHandle> OpenDatasetAndGetType(hid_t file, const string &filename,
                                                               const string &dataset_path) {
	std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
	H5DatasetHandle dataset;
	{
		H5ErrorSuppressor suppress;
		dataset = H5DatasetHandle(file, dataset_path.c_str());
	}

	if (!dataset.is_valid()) {
		throw IOException(FormatDatasetError("Failed to open dataset", filename, dataset_path));
	}

	auto type_id = H5Dget_type(dataset);
	if (type_id < 0) {
		throw IOException(FormatDatasetError("Failed to get dataset type", filename, dataset_path));
	}

	return {std::move(dataset), H5TypeHandle::TakeOwnershipOf(type_id)};
}

void ReadHDF5Strings(hid_t dataset_id, hid_t h5_type, hid_t mem_space, hid_t file_space, idx_t count,
                     const string &filename, const string &dataset_path,
                     std::function<void(idx_t, const string &)> callback) {
	if (count == 0) {
		return;
	}
	htri_t is_variable;
	H5T_cset_t cset;
	size_t str_len = 0;
	H5T_str_t strpad = H5T_STR_ERROR;
	{
		std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
		is_variable = H5Tis_variable_str(h5_type);
		if (is_variable < 0) {
			throw IOException(FormatDatasetError("Failed to inspect string type for dataset", filename, dataset_path));
		}
		cset = H5Tget_cset(h5_type);
		if (cset == H5T_CSET_ERROR) {
			throw IOException(
			    FormatDatasetError("Failed to inspect string charset for dataset", filename, dataset_path));
		}
		if (is_variable == 0) {
			str_len = H5Tget_size(h5_type);
			if (str_len == 0) {
				throw IOException(
				    FormatDatasetError("Failed to inspect string width for dataset", filename, dataset_path));
			}
			strpad = H5Tget_strpad(h5_type);
			if (strpad == H5T_STR_ERROR) {
				throw IOException(
				    FormatDatasetError("Failed to inspect string padding for dataset", filename, dataset_path));
			}
		}
	}

	if (is_variable > 0) {
		hsize_t reclaim_dim = count;
		H5DataspaceHandle reclaim_space(1, &reclaim_dim);
		if (!reclaim_space.is_valid()) {
			throw IOException(FormatDatasetError("Failed to create variable-length string reclaim dataspace", filename,
			                                     dataset_path));
		}
		std::vector<char *> string_data(count);

		herr_t status;
		{
			std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
			H5ErrorSuppressor suppress;
			status = H5Dread(dataset_id, h5_type, mem_space, file_space, H5P_DEFAULT, string_data.data());
		}

		if (status < 0) {
			throw IOException(FormatRemoteDatasetReadError(filename, dataset_path));
		}

		auto reclaim = [&]() {
			std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
			if (H5Dvlen_reclaim(h5_type, reclaim_space, H5P_DEFAULT, string_data.data()) < 0) {
				throw IOException(FormatDatasetError("Failed to reclaim variable-length string data from dataset",
				                                     filename, dataset_path));
			}
		};

		try {
			for (idx_t i = 0; i < count; i++) {
				if (string_data[i]) {
					callback(i, ValidateHDF5StringValue(string(string_data[i]), cset, filename, dataset_path));
				} else {
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
		return;
	}

	auto buffer_size = CheckedDatasetSizeProduct(count, static_cast<idx_t>(str_len), filename, dataset_path);
	std::vector<char> buffer(buffer_size);

	herr_t status;
	{
		std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
		H5ErrorSuppressor suppress;
		status = H5Dread(dataset_id, h5_type, mem_space, file_space, H5P_DEFAULT, buffer.data());
	}

	if (status < 0) {
		throw IOException(FormatRemoteDatasetReadError(filename, dataset_path));
	}

	for (idx_t i = 0; i < count; i++) {
		auto *str_ptr = buffer.data() + (i * str_len);
		auto decoded = H5DecodeFixedLengthString(str_ptr, str_len, strpad);
		callback(i, ValidateHDF5StringValue(std::move(decoded), cset, filename, dataset_path));
	}
}

} // namespace duckdb
