#pragma once

#include "duckdb.hpp"

namespace duckdb {

enum class H5RemoteBackendType : uint8_t { DUCKDB_FS, SFTP };

struct H5RemoteBackendDescriptor {
	H5RemoteBackendType type;
	std::string required_extension;
};

class H5RemoteBackend {
public:
	virtual ~H5RemoteBackend() = default;
	virtual idx_t GetFileSize() const = 0;
	virtual void Read(idx_t offset, idx_t size, void *buf) = 0;
};

bool IsH5RemotePath(const std::string &path);
H5RemoteBackendDescriptor DescribeH5RemotePath(const std::string &path);
unique_ptr<H5RemoteBackend> OpenH5RemoteBackend(ClientContext &context, const std::string &path);

} // namespace duckdb
