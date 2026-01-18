#pragma once

#include "hdf5.h"
#include "duckdb.hpp"

using namespace duckdb;

// ==================== HDF5 RAII Wrappers ====================
// These classes provide automatic resource management for HDF5 handles
// All wrappers are move-only to prevent accidental double-close bugs

// RAII wrapper for HDF5 error handler state
// Automatically disables HDF5 error printing on construction and restores it on destruction
class H5ErrorSuppressor {
	H5E_auto2_t old_func;
	void *old_client_data;

public:
	H5ErrorSuppressor() {
		H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
		H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);
	}

	~H5ErrorSuppressor() {
		H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);
	}

	// Disable copy and move to prevent double restoration
	H5ErrorSuppressor(const H5ErrorSuppressor &) = delete;
	H5ErrorSuppressor &operator=(const H5ErrorSuppressor &) = delete;
	H5ErrorSuppressor(H5ErrorSuppressor &&) = delete;
	H5ErrorSuppressor &operator=(H5ErrorSuppressor &&) = delete;
};

// RAII wrapper for HDF5 type handles
// Automatically closes type handle on destruction
// NOTE: Constructor copies the type (H5Tcopy) to ensure ownership
class H5TypeHandle {
	hid_t id;

	// Private constructor for taking ownership of existing handle
	struct TakeOwnership {};
	H5TypeHandle(hid_t type_id, TakeOwnership) : id(type_id) {
	}

public:
	H5TypeHandle() : id(-1) {
	}

	explicit H5TypeHandle(hid_t type_id) : id(H5Tcopy(type_id)) {
		if (id < 0) {
			throw IOException("Failed to copy HDF5 type");
		}
	}

	// Factory method to take ownership of an existing type handle
	// Use this for handles returned by H5Dget_type, H5Aget_type, H5Tget_super, etc.
	static H5TypeHandle TakeOwnershipOf(hid_t type_id) {
		return H5TypeHandle(type_id, TakeOwnership {});
	}

	~H5TypeHandle() {
		if (id >= 0) {
			H5Tclose(id);
		}
	}

	// Allow conversion to hid_t for use in HDF5 functions
	operator hid_t() const {
		return id;
	}

	hid_t get() const {
		return id;
	}

	// Disable copy but enable move
	H5TypeHandle(const H5TypeHandle &) = delete;
	H5TypeHandle &operator=(const H5TypeHandle &) = delete;

	H5TypeHandle(H5TypeHandle &&other) noexcept : id(other.id) {
		other.id = -1;
	}

	H5TypeHandle &operator=(H5TypeHandle &&other) noexcept {
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

// RAII wrapper for HDF5 file handles
// Automatically closes file handle on destruction
class H5FileHandle {
	hid_t id;

public:
	H5FileHandle() : id(-1) {
	}

	H5FileHandle(const char *filename, unsigned flags) {
		id = H5Fopen(filename, flags, H5P_DEFAULT);
	}

	~H5FileHandle() {
		if (id >= 0) {
			H5Fclose(id);
		}
	}

	// Allow conversion to hid_t for use in HDF5 functions
	operator hid_t() const {
		return id;
	}

	hid_t get() const {
		return id;
	}

	bool is_valid() const {
		return id >= 0;
	}

	// Disable copy but enable move
	H5FileHandle(const H5FileHandle &) = delete;
	H5FileHandle &operator=(const H5FileHandle &) = delete;

	H5FileHandle(H5FileHandle &&other) noexcept : id(other.id) {
		other.id = -1;
	}

	H5FileHandle &operator=(H5FileHandle &&other) noexcept {
		if (this != &other) {
			if (id >= 0) {
				H5Fclose(id);
			}
			id = other.id;
			other.id = -1;
		}
		return *this;
	}
};

// RAII wrapper for HDF5 dataset handles
// Automatically closes dataset handle on destruction
class H5DatasetHandle {
	hid_t id;

public:
	H5DatasetHandle() : id(-1) {
	}

	H5DatasetHandle(hid_t file_or_group_id, const char *path) {
		id = H5Dopen2(file_or_group_id, path, H5P_DEFAULT);
	}

	~H5DatasetHandle() {
		if (id >= 0) {
			H5Dclose(id);
		}
	}

	// Allow conversion to hid_t for use in HDF5 functions
	operator hid_t() const {
		return id;
	}

	hid_t get() const {
		return id;
	}

	bool is_valid() const {
		return id >= 0;
	}

	// Disable copy but enable move
	H5DatasetHandle(const H5DatasetHandle &) = delete;
	H5DatasetHandle &operator=(const H5DatasetHandle &) = delete;

	H5DatasetHandle(H5DatasetHandle &&other) noexcept : id(other.id) {
		other.id = -1;
	}

	H5DatasetHandle &operator=(H5DatasetHandle &&other) noexcept {
		if (this != &other) {
			if (id >= 0) {
				H5Dclose(id);
			}
			id = other.id;
			other.id = -1;
		}
		return *this;
	}
};

// RAII wrapper for HDF5 dataspace handles
// Automatically closes dataspace handle on destruction
class H5DataspaceHandle {
	hid_t id;

	// Private constructor for taking ownership of existing handle
	struct TakeOwnership {};
	H5DataspaceHandle(hid_t space_id, TakeOwnership) : id(space_id) {
	}

public:
	H5DataspaceHandle() : id(-1) {
	}

	// Constructor from dataset (calls H5Dget_space)
	explicit H5DataspaceHandle(hid_t dataset_id) {
		id = H5Dget_space(dataset_id);
	}

	// Constructor from dimensions (calls H5Screate_simple)
	H5DataspaceHandle(int rank, const hsize_t *dims) {
		id = H5Screate_simple(rank, dims, nullptr);
	}

	// Factory method to take ownership of an existing dataspace handle
	// Use this when you already have an hid_t from H5Aget_space, etc.
	static H5DataspaceHandle TakeOwnershipOf(hid_t space_id) {
		return H5DataspaceHandle(space_id, TakeOwnership {});
	}

	~H5DataspaceHandle() {
		if (id >= 0) {
			H5Sclose(id);
		}
	}

	// Allow conversion to hid_t for use in HDF5 functions
	operator hid_t() const {
		return id;
	}

	hid_t get() const {
		return id;
	}

	bool is_valid() const {
		return id >= 0;
	}

	// Disable copy but enable move
	H5DataspaceHandle(const H5DataspaceHandle &) = delete;
	H5DataspaceHandle &operator=(const H5DataspaceHandle &) = delete;

	H5DataspaceHandle(H5DataspaceHandle &&other) noexcept : id(other.id) {
		other.id = -1;
	}

	H5DataspaceHandle &operator=(H5DataspaceHandle &&other) noexcept {
		if (this != &other) {
			if (id >= 0) {
				H5Sclose(id);
			}
			id = other.id;
			other.id = -1;
		}
		return *this;
	}
};

// RAII wrapper for HDF5 attribute handles
// Automatically closes attribute handle on destruction
class H5AttributeHandle {
	hid_t id;

public:
	H5AttributeHandle() : id(-1) {
	}

	H5AttributeHandle(hid_t obj_id, const char *attr_name) {
		id = H5Aopen(obj_id, attr_name, H5P_DEFAULT);
	}

	~H5AttributeHandle() {
		if (id >= 0) {
			H5Aclose(id);
		}
	}

	// Allow conversion to hid_t for use in HDF5 functions
	operator hid_t() const {
		return id;
	}

	hid_t get() const {
		return id;
	}

	bool is_valid() const {
		return id >= 0;
	}

	// Disable copy but enable move
	H5AttributeHandle(const H5AttributeHandle &) = delete;
	H5AttributeHandle &operator=(const H5AttributeHandle &) = delete;

	H5AttributeHandle(H5AttributeHandle &&other) noexcept : id(other.id) {
		other.id = -1;
	}

	H5AttributeHandle &operator=(H5AttributeHandle &&other) noexcept {
		if (this != &other) {
			if (id >= 0) {
				H5Aclose(id);
			}
			id = other.id;
			other.id = -1;
		}
		return *this;
	}
};

// RAII wrapper for HDF5 object handles
// Automatically closes object handle on destruction
class H5ObjectHandle {
	hid_t id;

public:
	H5ObjectHandle() : id(-1) {
	}

	H5ObjectHandle(hid_t loc_id, const char *path) {
		id = H5Oopen(loc_id, path, H5P_DEFAULT);
	}

	~H5ObjectHandle() {
		if (id >= 0) {
			H5Oclose(id);
		}
	}

	// Allow conversion to hid_t for use in HDF5 functions
	operator hid_t() const {
		return id;
	}

	hid_t get() const {
		return id;
	}

	bool is_valid() const {
		return id >= 0;
	}

	// Disable copy but enable move
	H5ObjectHandle(const H5ObjectHandle &) = delete;
	H5ObjectHandle &operator=(const H5ObjectHandle &) = delete;

	H5ObjectHandle(H5ObjectHandle &&other) noexcept : id(other.id) {
		other.id = -1;
	}

	H5ObjectHandle &operator=(H5ObjectHandle &&other) noexcept {
		if (this != &other) {
			if (id >= 0) {
				H5Oclose(id);
			}
			id = other.id;
			other.id = -1;
		}
		return *this;
	}
};
