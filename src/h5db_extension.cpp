#define DUCKDB_EXTENSION_MAIN

#include "h5db_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// HDF5 linked through vcpkg
#include <hdf5.h>

// H5DB functions
#include "h5_functions.hpp"
#include "h5_internal.hpp"

namespace duckdb {

inline void H5dbVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		unsigned majnum, minnum, relnum;
		H5get_libversion(&majnum, &minnum, &relnum);

		std::string version_str = "H5db " + name.GetString() + ", HDF5 version " + std::to_string(majnum) + "." +
		                          std::to_string(minnum) + "." + std::to_string(relnum);
		return StringVector::AddString(result, version_str);
	});
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
	// Register HDF5 version check function
	auto h5db_version_scalar_function =
	    ScalarFunction("h5db_version", {LogicalType::VARCHAR}, LogicalType::VARCHAR, H5dbVersionScalarFun);
	loader.RegisterFunction(h5db_version_scalar_function);

	// Extension settings
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption("h5db_swmr_default", "Default to SWMR read mode for h5db table functions",
	                          LogicalType::BOOLEAN, Value(false));
	config.AddExtensionOption("h5db_batch_size", "Target batch size for h5_read chunk caching (e.g. 1MB, 8MB)",
	                          LogicalType::VARCHAR, Value(H5DB_DEFAULT_BATCH_SIZE_SETTING), SetH5dbBatchSize);

	// Register HDF5 table functions
	RegisterH5TreeFunction(loader);
	RegisterH5LsFunctions(loader);
	RegisterH5ReadFunction(loader);
	RegisterH5RseFunction(loader);
	RegisterH5AliasFunction(loader);
	RegisterH5AttrFunction(loader);
	RegisterH5IndexFunction(loader);
	RegisterH5AttributesFunction(loader);
}

void H5dbExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string H5dbExtension::Name() {
	return "h5db";
}

std::string H5dbExtension::Version() const {
#ifdef EXT_VERSION_H5DB
	return EXT_VERSION_H5DB;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(h5db, loader) {
	duckdb::LoadInternal(loader);
}
}
