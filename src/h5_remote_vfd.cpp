#include "h5_remote_vfd.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "h5_internal.hpp"

#include <hdf5.h>
#include <H5FDdevelop.h>
#include <cstring>
#include <mutex>
#include <unordered_map>
#include <list>

namespace duckdb {

static constexpr H5FD_class_value_t DUCKDB_VFD_VALUE = 600;
static constexpr idx_t METADATA_CACHE_BLOCK_SIZE = 1048576;
static constexpr idx_t METADATA_CACHE_MAX_BLOCKS = 128;
static hid_t duckdb_vfd_driver_id = -1;
static std::once_flag duckdb_vfd_register_once;
static thread_local ClientContext *duckdb_vfd_open_context = nullptr;

struct DuckDBVFDConfig {
	ClientContext *context;
};

struct H5FD_duckdb_t : public H5FD_t {
	struct MetadataCacheBlock {
		shared_ptr<BlockHandle> block_handle;
		idx_t valid_bytes = 0;
		std::list<idx_t>::iterator lru_it;
	};
	using MetadataCacheMap = std::unordered_map<idx_t, MetadataCacheBlock>;

	unique_ptr<FileHandle> handle;
	BufferManager *buffer_manager = nullptr;
	std::string path;
	haddr_t eof;
	haddr_t eoa;
	MetadataCacheMap metadata_cache_blocks;
	std::list<idx_t> metadata_cache_lru;
};

static inline H5FD_duckdb_t *GetFile(H5FD_t *f) {
	return reinterpret_cast<H5FD_duckdb_t *>(f);
}

static inline const H5FD_duckdb_t *GetFile(const H5FD_t *f) {
	return reinterpret_cast<const H5FD_duckdb_t *>(f);
}

static inline idx_t CacheBlockId(idx_t offset) {
	return offset / METADATA_CACHE_BLOCK_SIZE;
}

static void TouchMetadataBlock(H5FD_duckdb_t &file, H5FD_duckdb_t::MetadataCacheMap::iterator it) {
	file.metadata_cache_lru.erase(it->second.lru_it);
	file.metadata_cache_lru.push_front(it->first);
	it->second.lru_it = file.metadata_cache_lru.begin();
}

static void EvictMetadataBlockIfNeeded(H5FD_duckdb_t &file) {
	while (file.metadata_cache_blocks.size() >= METADATA_CACHE_MAX_BLOCKS && !file.metadata_cache_lru.empty()) {
		auto evict_block_id = file.metadata_cache_lru.back();
		file.metadata_cache_lru.pop_back();
		file.metadata_cache_blocks.erase(evict_block_id);
	}
}

static H5FD_duckdb_t::MetadataCacheBlock &LoadMetadataBlock(H5FD_duckdb_t &file, idx_t block_id) {
	auto it = file.metadata_cache_blocks.find(block_id);
	if (it != file.metadata_cache_blocks.end()) {
		TouchMetadataBlock(file, it);
		return it->second;
	}

	EvictMetadataBlockIfNeeded(file);

	auto block_offset = block_id * METADATA_CACHE_BLOCK_SIZE;
	auto bytes_to_read = MinValue<idx_t>(METADATA_CACHE_BLOCK_SIZE, static_cast<idx_t>(file.eof) - block_offset);
	if (bytes_to_read == 0) {
		throw IOException("Failed to load metadata cache block: zero bytes available");
	}
	if (!file.buffer_manager) {
		throw IOException("Failed to load metadata cache block: no buffer manager");
	}

	H5FD_duckdb_t::MetadataCacheBlock block;
	auto pinned_buffer = file.buffer_manager->Allocate(MemoryTag::EXTENSION, bytes_to_read);
	block.block_handle = pinned_buffer.GetBlockHandle();
	if (!block.block_handle) {
		throw IOException("Failed to load metadata cache block: allocation returned invalid block handle");
	}
	auto *source_handle = file.handle.get();
	if (!source_handle) {
		throw IOException("Failed to load metadata cache block: no readable file handle");
	}
	source_handle->Read(pinned_buffer.Ptr(), bytes_to_read, block_offset);
	block.valid_bytes = bytes_to_read;
	file.metadata_cache_lru.push_front(block_id);
	block.lru_it = file.metadata_cache_lru.begin();

	auto inserted = file.metadata_cache_blocks.emplace(block_id, std::move(block));
	return inserted.first->second;
}

static void CopyFromMetadataBlock(H5FD_duckdb_t &file, H5FD_duckdb_t::MetadataCacheBlock &block, idx_t offset_in_block,
                                  char *out, idx_t to_copy) {
	if (!file.buffer_manager) {
		throw IOException("Failed to read metadata cache block: no buffer manager");
	}
	if (!block.block_handle) {
		throw IOException("Failed to read metadata cache block: invalid block handle");
	}
	auto pinned_block = file.buffer_manager->Pin(block.block_handle);
	std::memcpy(out, pinned_block.Ptr() + offset_in_block, to_copy);
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
		auto buffered_handle = fs.OpenFile(name, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
		if (!buffered_handle) {
			return nullptr;
		}

		auto file = new H5FD_duckdb_t();
		file->handle = std::move(buffered_handle);
		file->buffer_manager = &BufferManager::GetBufferManager(*context);
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

static herr_t DuckDBRead(H5FD_t *file, H5FD_mem_t mem_type, hid_t, haddr_t addr, size_t size, void *buf) {
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
		auto read_size = static_cast<idx_t>(size);
		auto read_offset = static_cast<idx_t>(addr);
		// `DRAW` corresponds to raw dataset payload. In h5db scans this path is predominantly streaming, so bypass
		// cache.
		if (mem_type == H5FD_MEM_DRAW) {
			f->handle->Read(buf, read_size, read_offset);
			return 0;
		}

		auto *out = static_cast<char *>(buf);
		idx_t remaining = read_size;
		idx_t current_offset = read_offset;
		while (remaining > 0) {
			auto block_id = CacheBlockId(current_offset);
			auto &block = LoadMetadataBlock(*f, block_id);
			auto block_offset = block_id * METADATA_CACHE_BLOCK_SIZE;
			auto offset_in_block = current_offset - block_offset;
			if (offset_in_block >= block.valid_bytes) {
				return -1;
			}
			auto bytes_in_block = block.valid_bytes - offset_in_block;
			auto to_copy = MinValue<idx_t>(remaining, bytes_in_block);
			CopyFromMetadataBlock(*f, block, offset_in_block, out, to_copy);
			out += to_copy;
			current_offset += to_copy;
			remaining -= to_copy;
		}
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
