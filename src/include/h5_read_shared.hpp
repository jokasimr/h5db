#pragma once

#include "h5_raii.hpp"
#include "duckdb.hpp"
#include <functional>
#include <utility>

namespace duckdb {

string FormatRemoteHDF5Error(const string &prefix, const string &filename);
string FormatDatasetError(const string &prefix, const string &filename, const string &dataset_path);
string FormatRemoteDatasetReadError(const string &filename, const string &dataset_path);

idx_t CheckedDatasetSizeProduct(idx_t left, idx_t right, const string &filename, const string &dataset_path);

std::pair<H5DatasetHandle, H5TypeHandle> OpenDatasetAndGetType(hid_t file, const string &filename,
                                                               const string &dataset_path);

void ReadHDF5Strings(hid_t dataset_id, hid_t h5_type, hid_t mem_space, hid_t file_space, idx_t count,
                     const string &filename, const string &dataset_path,
                     std::function<void(idx_t, const string &)> callback);

} // namespace duckdb
