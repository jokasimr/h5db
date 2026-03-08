#include "h5_remote_vfd.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "h5_internal.hpp"

#include <hdf5.h>
#include <H5FDdevelop.h>
#include <cstring>
#include <mutex>

namespace duckdb {

static constexpr H5FD_class_value_t DUCKDB_VFD_VALUE = 600;
static hid_t duckdb_vfd_driver_id = -1;
static std::once_flag duckdb_vfd_register_once;
static thread_local ClientContext *duckdb_vfd_open_context = nullptr;

struct DuckDBVFDConfig {
	ClientContext *context;
};

struct H5FD_duckdb_t : public H5FD_t {
	unique_ptr<FileHandle> handle;
	std::string path;
	haddr_t eof;
	haddr_t eoa;
};

static inline H5FD_duckdb_t *GetFile(H5FD_t *f) {
	return reinterpret_cast<H5FD_duckdb_t *>(f);
}

static inline const H5FD_duckdb_t *GetFile(const H5FD_t *f) {
	return reinterpret_cast<const H5FD_duckdb_t *>(f);
}

static void *DuckDBFAPLCopy(const void *fapl) {
	if (!fapl) {
		return nullptr;
	}
	auto out = new DuckDBVFDConfig();
	*out = *reinterpret_cast<const DuckDBVFDConfig *>(fapl);
	return out;
}

static herr_t DuckDBFAPLFree(void *fapl) {
	delete reinterpret_cast<DuckDBVFDConfig *>(fapl);
	return 0;
}

static H5FD_t *DuckDBOpen(const char *name, unsigned flags, hid_t fapl_id, haddr_t) {
	if (!name) {
		return nullptr;
	}
	if ((flags & H5F_ACC_RDWR) || (flags & H5F_ACC_TRUNC) || (flags & H5F_ACC_CREAT)) {
		return nullptr;
	}

	const auto *config = reinterpret_cast<const DuckDBVFDConfig *>(H5Pget_driver_info(fapl_id));
	ClientContext *context = config ? config->context : nullptr;
	if (!context) {
		context = H5RemoteVFD::GetOpenContext();
	}
	if (!context) {
		return nullptr;
	}

	try {
		auto &fs = FileSystem::GetFileSystem(*context);
		auto handle = fs.OpenFile(name, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
		if (!handle) {
			return nullptr;
		}

		auto file = new H5FD_duckdb_t();
		file->handle = std::move(handle);
		file->path = name;
		file->eof = static_cast<haddr_t>(file->handle->GetFileSize());
		file->eoa = file->eof;
		return reinterpret_cast<H5FD_t *>(file);
	} catch (...) {
		return nullptr;
	}
}

static herr_t DuckDBClose(H5FD_t *file) {
	delete GetFile(file);
	return 0;
}

static int DuckDBCmp(const H5FD_t *a, const H5FD_t *b) {
	const auto &lhs = GetFile(a)->path;
	const auto &rhs = GetFile(b)->path;
	if (lhs == rhs) {
		return 0;
	}
	return lhs < rhs ? -1 : 1;
}

static herr_t DuckDBQuery(const H5FD_t *, unsigned long *flags) {
	if (flags) {
		*flags = 0;
	}
	return 0;
}

static haddr_t DuckDBGetEOA(const H5FD_t *file, H5FD_mem_t) {
	return GetFile(file)->eoa;
}

static herr_t DuckDBSetEOA(H5FD_t *file, H5FD_mem_t, haddr_t addr) {
	GetFile(file)->eoa = addr;
	return 0;
}

static haddr_t DuckDBGetEOF(const H5FD_t *file, H5FD_mem_t) {
	return GetFile(file)->eof;
}

static herr_t DuckDBRead(H5FD_t *file, H5FD_mem_t, hid_t, haddr_t addr, size_t size, void *buf) {
	if (size == 0) {
		return 0;
	}

	auto *f = GetFile(file);
	if (!f || !f->handle || !buf) {
		return -1;
	}

	if (addr > f->eof || size > static_cast<size_t>(f->eof - addr)) {
		return -1;
	}

	try {
		f->handle->Read(buf, static_cast<idx_t>(size), static_cast<idx_t>(addr));
		return 0;
	} catch (...) {
		return -1;
	}
}

static herr_t DuckDBWrite(H5FD_t *, H5FD_mem_t, hid_t, haddr_t, size_t, const void *) {
	return -1;
}

static herr_t DuckDBTruncate(H5FD_t *, hid_t, hbool_t) {
	return 0;
}

static herr_t DuckDBLock(H5FD_t *, hbool_t) {
	return 0;
}

static herr_t DuckDBUnlock(H5FD_t *) {
	return 0;
}

static const H5FD_class_t &DuckDBVFDClass() {
	static H5FD_class_t cls;
	static std::once_flag init_once;
	std::call_once(init_once, []() {
		memset(&cls, 0, sizeof(cls));
		cls.version = H5FD_CLASS_VERSION;
		cls.value = DUCKDB_VFD_VALUE;
		cls.name = "duckdb_httpfs";
		cls.maxaddr = HADDR_MAX;
		cls.fc_degree = H5F_CLOSE_WEAK;
		cls.fapl_size = sizeof(DuckDBVFDConfig);
		cls.fapl_copy = DuckDBFAPLCopy;
		cls.fapl_free = DuckDBFAPLFree;
		cls.open = DuckDBOpen;
		cls.close = DuckDBClose;
		cls.cmp = DuckDBCmp;
		cls.query = DuckDBQuery;
		cls.get_eoa = DuckDBGetEOA;
		cls.set_eoa = DuckDBSetEOA;
		cls.get_eof = DuckDBGetEOF;
		cls.read = DuckDBRead;
		cls.write = DuckDBWrite;
		cls.truncate = DuckDBTruncate;
		cls.lock = DuckDBLock;
		cls.unlock = DuckDBUnlock;
		H5FD_mem_t single_map[H5FD_MEM_NTYPES] = H5FD_FLMAP_SINGLE;
		memcpy(cls.fl_map, single_map, sizeof(single_map));
	});
	return cls;
}

static hid_t GetDriver() {
	std::call_once(duckdb_vfd_register_once, []() {
		std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
		duckdb_vfd_driver_id = H5FDregister(&DuckDBVFDClass());
		if (duckdb_vfd_driver_id < 0) {
			throw IOException("Failed to register DuckDB HTTPFS HDF5 VFD");
		}
	});
	return duckdb_vfd_driver_id;
}

bool H5RemoteVFD::IsRemotePath(const std::string &path) {
	auto lower_path = StringUtil::Lower(path);
	return StringUtil::StartsWith(lower_path, "http://") || StringUtil::StartsWith(lower_path, "https://") ||
	       StringUtil::StartsWith(lower_path, "s3://") || StringUtil::StartsWith(lower_path, "s3a://") ||
	       StringUtil::StartsWith(lower_path, "s3n://") || StringUtil::StartsWith(lower_path, "r2://") ||
	       StringUtil::StartsWith(lower_path, "gcs://") || StringUtil::StartsWith(lower_path, "gs://") ||
	       StringUtil::StartsWith(lower_path, "hf://");
}

void H5RemoteVFD::ConfigureFAPL(ClientContext &context, hid_t fapl_id) {
	DuckDBVFDConfig config;
	config.context = &context;

	if (H5Pset_driver(fapl_id, GetDriver(), &config) < 0) {
		throw IOException("Failed to configure DuckDB HTTPFS HDF5 VFD");
	}
}

void H5RemoteVFD::SetOpenContext(ClientContext *context) {
	duckdb_vfd_open_context = context;
}

ClientContext *H5RemoteVFD::GetOpenContext() {
	return duckdb_vfd_open_context;
}

} // namespace duckdb
