#pragma once

#include "duckdb.hpp"
#include "h5_raii.hpp"
#include <array>
#include <compare>
#include <optional>
#include <vector>

namespace duckdb {

enum class H5TreeEntryType : uint8_t { UNKNOWN, GROUP, DATASET, DATATYPE, LINK, EXTERNAL };

struct H5TreeProjectedAttributeSpec {
	std::string attribute_name;
	std::string output_column_name;
	Value default_value;
	LogicalType output_type;
};

struct H5TreeObjectIdentity {
	unsigned long fileno = 0;
	std::array<unsigned char, sizeof(H5O_token_t)> token {};

	auto operator<=>(const H5TreeObjectIdentity &) const = default;
};

struct H5TreeProjectedAttributeValue {
	bool present = false;
	Value value;
};

struct H5TreeRow {
	std::string path;
	std::optional<std::string> type;
	std::optional<std::string> dtype;
	std::vector<hsize_t> shape;
	bool has_shape = false;
	std::vector<H5TreeProjectedAttributeValue> projected_values;
};

struct H5TreeResolvedEntry {
	H5TreeEntryType type_kind = H5TreeEntryType::UNKNOWN;
	std::optional<H5TreeObjectIdentity> identity;
	bool traversable_group = false;
	bool is_soft_link = false;
};

struct H5TreeNamedRow {
	std::string name;
	H5TreeRow row;
};

std::string H5TreeNormalizeObjectPath(std::string object_path);
std::string H5TreeNormalizeExceptionMessage(const std::string &message);
bool H5TreeIsProjectedAttributeArgument(const Value &input);
H5TreeProjectedAttributeSpec H5TreeParseProjectedAttributeSpec(const Value &input, const std::string &function_name);
void H5TreeBindProjectedAttributes(const std::string &function_name, const vector<Value> &inputs, idx_t start_idx,
                                   vector<string> &names, vector<LogicalType> &return_types,
                                   vector<H5TreeProjectedAttributeSpec> &projected_attributes);
std::optional<std::string> H5TreeTypeName(H5TreeEntryType type);
bool H5TreeCanHaveProjectedAttributes(H5TreeEntryType type);
void H5TreeWriteRow(const H5TreeRow &row, const vector<H5TreeProjectedAttributeSpec> &projected_attributes,
                    DataChunk &chunk, idx_t row_idx, idx_t &shape_offset, uint64_t *shape_data);

class H5TreeFileReader {
public:
	H5TreeFileReader(ClientContext &context, const std::string &filename, bool swmr,
	                 const vector<H5TreeProjectedAttributeSpec> &projected_attributes);

	H5FileHandle &GetFileHandle() {
		return file;
	}

	const H5TreeObjectIdentity &GetRootIdentity() const {
		return root_identity;
	}

	const std::string &GetFilename() const {
		return filename;
	}

	const vector<H5TreeProjectedAttributeSpec> &GetProjectedAttributes() const {
		return projected_attributes;
	}

	H5TreeResolvedEntry ResolveEntry(const std::string &path, const H5L_info2_t &link_info, hid_t parent_loc,
	                                 const char *link_name);
	void PopulateRowMetadataAndAttributes(H5TreeRow &row, H5TreeEntryType type_kind,
	                                      const H5TreeObjectIdentity &identity, const std::string &path,
	                                      hid_t parent_loc, const char *link_name);

private:
	H5ObjectHandle OpenObjectByIdentity(const H5TreeObjectIdentity &identity, const std::string &path);
	H5ObjectHandle OpenObject(const H5TreeObjectIdentity &identity, const std::string &path, hid_t parent_loc,
	                          const char *link_name);
	bool ResolveObjectInfo(H5O_info2_t &info, const std::optional<H5TreeObjectIdentity> &identity,
	                       const std::string &path, hid_t parent_loc, const char *link_name);
	void InitializeRootIdentity();
	static H5TreeEntryType EntryTypeFromObjectInfo(const H5O_info2_t &info);

private:
	ClientContext &context;
	const std::string &filename;
	const vector<H5TreeProjectedAttributeSpec> &projected_attributes;
	H5FileHandle file;
	H5TreeObjectIdentity root_identity;
};

void H5TreeListImmediateEntries(H5TreeFileReader &reader, const std::string &group_path,
                                std::vector<H5TreeNamedRow> &rows);

} // namespace duckdb
