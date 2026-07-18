#pragma once

#include "duckdb.hpp"
#include "h5_raii.hpp"
#include <optional>
#include <vector>

namespace duckdb {

enum class H5TreeEntryType : uint8_t { UNKNOWN, GROUP, DATASET, DATATYPE, LINK, EXTERNAL };

struct H5TreeProjectedAttributeSpec {
	bool all_attributes = false;
	std::string attribute_name;
	std::string output_column_name;
	Value default_value;
	LogicalType output_type;
};

struct H5TreeObjectIdentity {
	unsigned long fileno = 0;
	H5O_token_t token {};
};

struct H5TreeProjectedAttributeValue {
	bool present = false;
	Value value;
};

struct H5TreeReadOptions {
	bool read_type = false;
	bool read_dtype = false;
	bool read_shape = false;
	vector<idx_t> projected_attribute_ids;
};

struct H5TreeRow {
	std::string path;
	std::optional<std::string> type;
	std::optional<std::string> dtype;
	std::optional<std::vector<hsize_t>> shape;
	std::vector<H5TreeProjectedAttributeValue> projected_values;
};

struct H5TreeResolvedEntry {
	H5TreeEntryType type_kind = H5TreeEntryType::UNKNOWN;
	std::optional<H5TreeObjectIdentity> identity;
};

struct H5TreeNamedRow {
	std::string name;
	H5TreeRow row;
};

std::string H5TreeNormalizeObjectPath(std::string object_path);
bool H5TreeIsProjectedAttributeArgument(const Value &input);
H5TreeProjectedAttributeSpec H5TreeParseProjectedAttributeSpec(const Value &input, const std::string &function_name);
void H5TreeBindProjectedAttributes(const std::string &function_name, const vector<Value> &inputs, idx_t start_idx,
                                   vector<string> &names, vector<LogicalType> &return_types,
                                   vector<H5TreeProjectedAttributeSpec> &projected_attributes);
std::optional<std::string> H5TreeTypeName(H5TreeEntryType type);
void H5TreeWriteProjectedValue(const H5TreeRow &row, const vector<H5TreeProjectedAttributeSpec> &projected_attributes,
                               column_t column_id, Vector &vector, idx_t row_idx, idx_t &shape_offset,
                               uint64_t *shape_data);
Value H5ReadAllAttributesMapValue(hid_t object_id);
H5TreeReadOptions H5TreeReadAll(idx_t projected_attribute_count);

class H5TreeFileReader {
public:
	H5TreeFileReader(ClientContext &context, const std::string &filename, bool swmr,
	                 const vector<H5TreeProjectedAttributeSpec> &projected_attributes, H5TreeReadOptions read_options);

	H5FileHandle &GetFileHandle() {
		return file;
	}

	const std::string &GetFilename() const {
		return filename;
	}

	bool ReadsType() const {
		return read_options.read_type;
	}

	bool NeedsEntryResolution() const {
		return read_options.read_type || read_options.read_dtype || read_options.read_shape ||
		       !read_options.projected_attribute_ids.empty();
	}

	void InitializeProjectedValues(H5TreeRow &row) const {
		if (!read_options.projected_attribute_ids.empty()) {
			row.projected_values.resize(projected_attributes.size());
		}
	}

	H5TreeObjectIdentity GetRootIdentity();
	bool SameObject(const H5TreeObjectIdentity &lhs, const H5TreeObjectIdentity &rhs) const;
	H5TreeResolvedEntry ResolveEntry(const H5L_info2_t &link_info, hid_t parent_loc, const char *link_name);
	void PopulateRowMetadataAndAttributes(H5TreeRow &row, H5TreeEntryType type_kind, const std::string &path,
	                                      hid_t parent_loc, const char *link_name);

private:
	H5ObjectHandle OpenObject(const std::string &path, hid_t parent_loc, const char *link_name);
	static H5TreeEntryType EntryTypeFromObjectInfo(const H5O_info2_t &info);

private:
	const std::string &filename;
	const vector<H5TreeProjectedAttributeSpec> &projected_attributes;
	H5TreeReadOptions read_options;
	H5FileHandle file;
};

void H5TreeListImmediateEntries(H5TreeFileReader &reader, const std::string &group_path,
                                std::vector<H5TreeNamedRow> &rows);
void H5TreeValidateListGroup(H5TreeFileReader &reader, const std::string &group_path);
bool H5TreeListEntriesBatch(H5TreeFileReader &reader, const std::string &group_path, hsize_t &idx, idx_t max_rows,
                            std::vector<H5TreeNamedRow> &rows);

} // namespace duckdb
