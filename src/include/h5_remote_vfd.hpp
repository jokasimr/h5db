#pragma once

#include "duckdb.hpp"
#include "hdf5.h"

namespace duckdb {

struct H5RemoteErrorInfo {
	std::string message;
	bool interrupted = false;
};

class H5RemoteVFD {
public:
	static bool IsRemotePath(const std::string &path);
	static std::string GetRequiredExtension(const std::string &path);
	static void ConfigureFAPL(ClientContext &context, hid_t fapl_id);
	static void SetOpenContext(ClientContext *context);
	static ClientContext *GetOpenContext();
	static void ClearLastError();
	static H5RemoteErrorInfo TakeLastErrorInfo();
	static std::string TakeLastError();
};

} // namespace duckdb
