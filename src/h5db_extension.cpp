#define DUCKDB_EXTENSION_MAIN

#include "h5db_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

// HDF5 linked through vcpkg
#include <hdf5.h>

// H5DB functions
#include "h5_functions.hpp"

namespace duckdb {

inline void H5dbScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "H5db " + name.GetString() + " 🐥");
	});
}

inline void H5dbOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "H5db " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

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

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto h5db_scalar_function = ScalarFunction("h5db", {LogicalType::VARCHAR}, LogicalType::VARCHAR, H5dbScalarFun);
	loader.RegisterFunction(h5db_scalar_function);

	// Register another scalar function
	auto h5db_openssl_version_scalar_function = ScalarFunction("h5db_openssl_version", {LogicalType::VARCHAR},
	                                                           LogicalType::VARCHAR, H5dbOpenSSLVersionScalarFun);
	loader.RegisterFunction(h5db_openssl_version_scalar_function);

	// Register HDF5 version check function
	auto h5db_version_scalar_function =
	    ScalarFunction("h5db_version", {LogicalType::VARCHAR}, LogicalType::VARCHAR, H5dbVersionScalarFun);
	loader.RegisterFunction(h5db_version_scalar_function);

	// Register HDF5 table functions
	RegisterH5TreeFunction(loader);
	RegisterH5ReadFunction(loader);
	RegisterH5RseFunction(loader);
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
