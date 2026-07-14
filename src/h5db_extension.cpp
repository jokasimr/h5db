#define DUCKDB_EXTENSION_MAIN

#include "h5db_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#if __has_include("duckdb/common/vector/flat_vector.hpp")
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#else
#include "duckdb/common/types/vector.hpp"
#endif

// External libraries linked through vcpkg
#include <hdf5.h>
#include <libssh2.h>

// H5DB functions
#include "h5_functions.hpp"
#include "h5_internal.hpp"

namespace duckdb {

static std::string H5dbExtensionVersionString() {
#ifdef EXT_VERSION_H5DB
	return EXT_VERSION_H5DB;
#else
	return "";
#endif
}

static std::string H5dbSftpLibraryVersionString() {
	const auto *version = libssh2_version(0);
	return version ? version : LIBSSH2_VERSION;
}

static void H5dbVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	unsigned majnum, minnum, relnum;
	H5get_libversion(&majnum, &minnum, &relnum);
	auto hdf5_version = std::to_string(majnum) + "." + std::to_string(minnum) + "." + std::to_string(relnum);
	auto h5db_version = H5dbExtensionVersionString();
	auto sftp_library_version = H5dbSftpLibraryVersionString();

	auto &children = StructVector::GetEntries(result);
	D_ASSERT(children.size() == 3);
	auto &h5db_child = *children[0];
	auto &hdf5_child = *children[1];
	auto &sftp_library_child = *children[2];

	for (idx_t i = 0; i < args.size(); i++) {
		FlatVector::GetData<string_t>(h5db_child)[i] = StringVector::AddString(h5db_child, h5db_version);
		FlatVector::GetData<string_t>(hdf5_child)[i] = StringVector::AddString(hdf5_child, hdf5_version);
		FlatVector::GetData<string_t>(sftp_library_child)[i] =
		    StringVector::AddString(sftp_library_child, sftp_library_version);
	}

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	result.Verify(args.size());
}

static void SetH5dbBatchSize(ClientContext &context, SetScope scope, Value &parameter) {
	auto input = parameter.ToString();
	auto parsed = ParseBatchSizeSetting(parameter);
	if (parsed > H5DB_MAX_BATCH_SIZE_BYTES) {
		parameter = Value(H5DB_MAX_BATCH_SIZE_SETTING);
	} else {
		parameter = Value(input);
	}
}

static void LoadInternal(ExtensionLoader &loader) {
	child_list_t<LogicalType> version_struct_children = {
	    {"h5db_version", LogicalType::VARCHAR},
	    {"hdf5_version", LogicalType::VARCHAR},
	    {"sftp_library_version", LogicalType::VARCHAR},
	};
	auto h5db_version_scalar_function =
	    ScalarFunction("h5db_version", {}, LogicalType::STRUCT(version_struct_children), H5dbVersionScalarFun);
	CreateScalarFunctionInfo h5db_version_info(std::move(h5db_version_scalar_function));
	h5db_version_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	h5db_version_info.descriptions.push_back(H5FunctionDescription(
	    {}, {}, "Returns h5db, linked HDF5, and SFTP backend library versions.", {"SELECT h5db_version()"}));
	loader.RegisterFunction(std::move(h5db_version_info));

	// Extension settings
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption("h5db_swmr_default", "Default to SWMR read mode for h5db table functions",
	                          LogicalType::BOOLEAN, Value(false));
	config.AddExtensionOption("h5db_batch_size", "Target batch size for h5_read numeric caching (e.g. 1MB, 8MB)",
	                          LogicalType::VARCHAR, Value(H5DB_DEFAULT_BATCH_SIZE_SETTING), SetH5dbBatchSize);

	// Register HDF5 table functions
	RegisterH5TreeFunction(loader);
	RegisterH5LsFunctions(loader);
	RegisterH5ReadFunction(loader);
	RegisterH5RseFunction(loader);
	RegisterH5ReeFunction(loader);
	RegisterH5AliasFunction(loader);
	RegisterH5AttrFunction(loader);
	RegisterH5IndexFunction(loader);
	RegisterH5FirstFileFunction(loader);
	RegisterH5AttributesFunction(loader);
	RegisterH5SftpSecrets(loader);
}

void H5dbExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string H5dbExtension::Name() {
	return "h5db";
}

std::string H5dbExtension::Version() const {
	return H5dbExtensionVersionString();
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(h5db, loader) {
	duckdb::LoadInternal(loader);
}
}
