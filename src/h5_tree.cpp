#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "h5_raii.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#if __has_include("duckdb/common/vector/flat_vector.hpp")
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#else
#include "duckdb/common/types/vector.hpp"
#endif
#include <algorithm>
#include <array>
#include <atomic>
#include <condition_variable>
#include <chrono>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <vector>

namespace duckdb {

template <class T>
static T &GetTreeStructChild(T &child) {
	return child;
}

template <class T>
static T &GetTreeStructChild(unique_ptr<T> &child) {
	return *child;
}

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

	bool operator==(const H5TreeObjectIdentity &other) const {
		return fileno == other.fileno && token == other.token;
	}
};

struct H5TreeObjectIdentityHash {
	size_t operator()(const H5TreeObjectIdentity &identity) const {
		size_t hash = std::hash<unsigned long> {}(identity.fileno);
		for (auto byte : identity.token) {
			hash = CombineHash(hash, std::hash<unsigned char> {}(byte));
		}
		return hash;
	}
};

struct H5TreeProjectedAttributeValue {
	bool present = false;
	Value value;
};

struct H5TreeCachedObject {
	H5TreeEntryType type = H5TreeEntryType::UNKNOWN;
	bool type_loaded = false;
	std::string dtype;
	bool dtype_loaded = false;
	std::vector<hsize_t> shape;
	bool shape_loaded = false;
	std::vector<H5TreeProjectedAttributeValue> projected_values;
	std::vector<bool> projected_loaded;
};

struct H5TreeRow {
	std::string path;
	std::optional<std::string> type;
	std::optional<std::string> dtype;
	std::vector<hsize_t> shape;
	bool has_shape = false;
	std::vector<H5TreeProjectedAttributeValue> projected_values;
};

struct H5TreeAncestryNode {
	H5TreeObjectIdentity identity;
	std::shared_ptr<const H5TreeAncestryNode> parent;

	bool Contains(const H5TreeObjectIdentity &candidate) const {
		auto current = this;
		while (current) {
			if (current->identity == candidate) {
				return true;
			}
			current = current->parent.get();
		}
		return false;
	}
};

struct H5TreeBindData : public TableFunctionData {
	std::string filename;
	vector<H5TreeProjectedAttributeSpec> projected_attributes;
	vector<LogicalType> all_return_types;
	bool swmr = false;
};

struct H5TreeScanPlan {
	vector<column_t> column_ids;
	vector<idx_t> projection_ids;
	bool remove_filter_columns = false;
	bool need_type = false;
	bool need_dtype = false;
	bool need_shape = false;
	bool need_any_projected_attributes = false;
	vector<bool> need_projected_attributes;
	unique_ptr<TableFilter> path_filter;
	unique_ptr<TableFilter> type_filter;
	bool use_exact_path = false;
	std::string exact_path;
	std::string traversal_root = "/";
	std::optional<std::string> traversal_root_hint;
};

struct H5TreeGlobalState : public GlobalTableFunctionState {
	H5TreeScanPlan plan;
	unique_ptr<class H5TreeScanner> scanner;

	idx_t MaxThreads() const override {
		return 1;
	}
};

struct H5TreeLocalState : public LocalTableFunctionState {
	DataChunk all_columns;
};

struct H5TreeResolvedEntry {
	H5TreeEntryType type_kind = H5TreeEntryType::UNKNOWN;
	std::optional<H5TreeObjectIdentity> identity;
	bool traversable_group = false;
	bool cycle = false;
	bool is_soft_link = false;
	std::shared_ptr<const H5TreeAncestryNode> ancestry;
};

struct H5TreeVisitTask {
	std::string root_path;
	std::shared_ptr<const H5TreeAncestryNode> ancestry;
};

class H5TreeScanner;

struct H5TreeVisitContext {
	H5TreeScanner *scanner;
	std::string root_path;
	std::shared_ptr<const H5TreeAncestryNode> root_ancestry;
	std::unordered_map<std::string, std::shared_ptr<const H5TreeAncestryNode>> group_ancestry;
	std::deque<H5TreeVisitTask> deferred_tasks;
};

struct H5TreePathWalkResult {
	bool found = false;
	H5L_info2_t final_link_info;
	std::shared_ptr<const H5TreeAncestryNode> parent_ancestry;
	std::shared_ptr<const H5TreeAncestryNode> final_ancestry;
};

static bool H5TreeIsAliasStructType(const LogicalType &type) {
	if (type.id() != LogicalTypeId::STRUCT) {
		return false;
	}
	auto &children = StructType::GetChildTypes(type);
	return children.size() == 3 && children[0].second == LogicalType::VARCHAR;
}

static bool H5TreeIsAttrStructType(const LogicalType &type) {
	if (type.id() != LogicalTypeId::STRUCT) {
		return false;
	}
	auto &children = StructType::GetChildTypes(type);
	return children.size() == 3 && children[0].second == LogicalType::VARCHAR;
}

static Value H5TreeUnwrapAliasSpec(const Value &input, std::optional<std::string> &alias_name) {
	Value current = input;
	while (H5TreeIsAliasStructType(current.type())) {
		auto &children = StructValue::GetChildren(current);
		if (children.size() != 3 || children[0].GetValue<string>() != "__alias__") {
			break;
		}
		if (!alias_name) {
			alias_name = children[1].GetValue<string>();
		}
		current = children[2];
	}
	return current;
}

static H5TreeProjectedAttributeSpec H5TreeParseProjectedAttributeSpec(const Value &input) {
	std::optional<std::string> alias_name;
	auto current = H5TreeUnwrapAliasSpec(input, alias_name);
	if (!H5TreeIsAttrStructType(current.type())) {
		throw InvalidInputException("h5_tree projected attribute arguments must be h5_attr(name, default_value) or "
		                            "h5_alias(alias, h5_attr(...))");
	}
	auto &children = StructValue::GetChildren(current);
	if (children.size() != 3 || children[0].GetValue<string>() != "__attr__") {
		throw InvalidInputException("h5_tree projected attribute arguments must be h5_attr(name, default_value) or "
		                            "h5_alias(alias, h5_attr(...))");
	}
	if (children[1].IsNull()) {
		throw InvalidInputException("h5_attr name must not be NULL");
	}
	if (children[2].type().id() == LogicalTypeId::SQLNULL) {
		throw InvalidInputException(
		    "h5_attr default_value must have a concrete type; use an explicit cast such as NULL::VARCHAR");
	}

	H5TreeProjectedAttributeSpec spec;
	spec.attribute_name = children[1].GetValue<string>();
	spec.output_column_name = alias_name ? *alias_name : spec.attribute_name;
	spec.default_value = children[2];
	spec.output_type = children[2].type();
	return spec;
}

static std::string H5TreeNormalizeExceptionMessage(const std::string &message) {
	if (message.empty() || message.front() != '{') {
		return message;
	}
	try {
		auto info = StringUtil::ParseJSONMap(message, true)->Flatten();
		for (const auto &entry : info) {
			if (entry.first == "exception_message") {
				return entry.second;
			}
		}
	} catch (...) {
	}
	return message;
}

static std::optional<std::string> H5TreeTypeName(H5TreeEntryType type) {
	switch (type) {
	case H5TreeEntryType::GROUP:
		return std::string("group");
	case H5TreeEntryType::DATASET:
		return std::string("dataset");
	case H5TreeEntryType::DATATYPE:
		return std::string("datatype");
	case H5TreeEntryType::LINK:
		return std::string("link");
	case H5TreeEntryType::EXTERNAL:
		return std::string("external");
	case H5TreeEntryType::UNKNOWN:
		return std::nullopt;
	default:
		throw InternalException("Unknown h5_tree type");
	}
}

static bool H5TreeCanHaveProjectedAttributes(H5TreeEntryType type) {
	return type == H5TreeEntryType::GROUP || type == H5TreeEntryType::DATASET || type == H5TreeEntryType::DATATYPE;
}

static H5TreeObjectIdentity H5TreeIdentityFromToken(unsigned long fileno, const H5O_token_t &token) {
	H5TreeObjectIdentity identity;
	identity.fileno = fileno;
	std::memcpy(identity.token.data(), &token, sizeof(H5O_token_t));
	return identity;
}

static H5TreeObjectIdentity H5TreeIdentityFromObjectInfo(const H5O_info2_t &info) {
	return H5TreeIdentityFromToken(info.fileno, info.token);
}

static std::string H5TreeParentPath(const std::string &path) {
	if (path == "/") {
		return "/";
	}
	auto slash = path.rfind('/');
	if (slash == std::string::npos || slash == 0) {
		return "/";
	}
	return path.substr(0, slash);
}

static std::vector<std::string> H5TreeSplitAbsolutePath(const std::string &path) {
	std::vector<std::string> parts;
	if (path.empty() || path == "/") {
		return parts;
	}
	if (path[0] != '/') {
		throw InvalidInputException("h5_tree internal error: expected absolute path");
	}
	idx_t start = 1;
	while (start < path.size()) {
		auto end = path.find('/', start);
		if (end == std::string::npos) {
			parts.push_back(path.substr(start));
			break;
		}
		parts.push_back(path.substr(start, end - start));
		start = end + 1;
	}
	return parts;
}

static std::string H5TreeDeriveVisitedPath(const std::string &root_path, const char *name) {
	if (!name || !name[0] || (name[0] == '.' && !name[1])) {
		return root_path;
	}
	if (root_path == "/") {
		return "/" + std::string(name);
	}
	return root_path + "/" + name;
}

static bool H5TreeTryGetStringConstant(const Value &value, std::string &result) {
	if (value.IsNull() || value.type().id() != LogicalTypeId::VARCHAR) {
		return false;
	}
	result = value.GetValue<string>();
	return true;
}

static bool H5TreeEvaluateStringFilter(const TableFilter &filter, const std::optional<std::string> &value) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		if (!value) {
			return false;
		}
		auto &constant_filter = filter.Cast<ConstantFilter>();
		return constant_filter.Compare(Value(*value));
	}
	case TableFilterType::IN_FILTER: {
		if (!value) {
			return false;
		}
		auto &in_filter = filter.Cast<InFilter>();
		auto candidate = Value(*value);
		for (const auto &entry : in_filter.values) {
			if (Value::NotDistinctFrom(candidate, entry)) {
				return true;
			}
		}
		return false;
	}
	case TableFilterType::IS_NULL:
		return !value.has_value();
	case TableFilterType::IS_NOT_NULL:
		return value.has_value();
	case TableFilterType::CONJUNCTION_AND: {
		auto &and_filter = filter.Cast<ConjunctionAndFilter>();
		for (auto &child : and_filter.child_filters) {
			if (!H5TreeEvaluateStringFilter(*child, value)) {
				return false;
			}
		}
		return true;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &or_filter = filter.Cast<ConjunctionOrFilter>();
		for (auto &child : or_filter.child_filters) {
			if (H5TreeEvaluateStringFilter(*child, value)) {
				return true;
			}
		}
		return false;
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		return !optional_filter.child_filter || H5TreeEvaluateStringFilter(*optional_filter.child_filter, value);
	}
	case TableFilterType::DYNAMIC_FILTER:
	case TableFilterType::BLOOM_FILTER:
		return true;
	default:
		throw NotImplementedException("Unsupported pushed-down h5_tree string filter type: %d",
		                              static_cast<int>(filter.filter_type));
	}
}

static void H5TreeFlattenAndFilters(const TableFilter &filter, std::vector<const TableFilter *> &leaves) {
	if (filter.filter_type == TableFilterType::CONJUNCTION_AND) {
		for (auto &child : filter.Cast<ConjunctionAndFilter>().child_filters) {
			H5TreeFlattenAndFilters(*child, leaves);
		}
		return;
	}
	if (filter.filter_type == TableFilterType::OPTIONAL_FILTER) {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		if (optional_filter.child_filter) {
			H5TreeFlattenAndFilters(*optional_filter.child_filter, leaves);
		}
		return;
	}
	leaves.push_back(&filter);
}

static std::optional<std::string> H5TreeExtractExactPathValue(const TableFilter &filter) {
	std::vector<const TableFilter *> leaves;
	H5TreeFlattenAndFilters(filter, leaves);
	for (auto *leaf : leaves) {
		if (leaf->filter_type == TableFilterType::CONSTANT_COMPARISON) {
			auto &constant_filter = leaf->Cast<ConstantFilter>();
			if (constant_filter.comparison_type == ExpressionType::COMPARE_EQUAL) {
				std::string candidate;
				if (H5TreeTryGetStringConstant(constant_filter.constant, candidate)) {
					return candidate;
				}
			}
		} else if (leaf->filter_type == TableFilterType::IN_FILTER) {
			auto &in_filter = leaf->Cast<InFilter>();
			if (in_filter.values.size() == 1) {
				std::string candidate;
				if (H5TreeTryGetStringConstant(in_filter.values[0], candidate)) {
					return candidate;
				}
			}
		}
	}
	return std::nullopt;
}

static std::optional<std::string> H5TreeNormalizeAbsolutePrefix(const std::string &prefix) {
	if (prefix.empty() || prefix[0] != '/') {
		return std::nullopt;
	}
	auto trimmed = prefix;
	while (trimmed.size() > 1 && trimmed.back() == '/') {
		trimmed.pop_back();
	}
	return trimmed;
}

static std::optional<std::string> H5TreeIncrementLastByte(const std::string &value) {
	if (value.empty()) {
		return std::nullopt;
	}
	auto result = value;
	auto last = static_cast<unsigned char>(result.back());
	if (last == 255) {
		return std::nullopt;
	}
	result.back() = static_cast<char>(last + 1);
	return result;
}

static bool H5TreeMatchesPrefixUpperBound(const std::string &lower_bound, const std::string &upper_bound) {
	auto utf8_next = lower_bound;
	if (FilterCombiner::FindNextLegalUTF8(utf8_next) && utf8_next == upper_bound) {
		return true;
	}
	auto last_byte_next = H5TreeIncrementLastByte(lower_bound);
	return last_byte_next && *last_byte_next == upper_bound;
}

struct H5TreeTraversalHint {
	std::string fallback_root = "/";
	std::optional<std::string> rooted_prefix;
};

static H5TreeTraversalHint H5TreeExtractTraversalHint(const TableFilter &filter) {
	std::vector<const TableFilter *> leaves;
	H5TreeFlattenAndFilters(filter, leaves);
	H5TreeTraversalHint result;
	for (auto *lower_leaf : leaves) {
		if (lower_leaf->filter_type != TableFilterType::CONSTANT_COMPARISON) {
			continue;
		}
		auto &lower_filter = lower_leaf->Cast<ConstantFilter>();
		if (lower_filter.comparison_type != ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
			continue;
		}
		std::string lower;
		if (!H5TreeTryGetStringConstant(lower_filter.constant, lower)) {
			continue;
		}
		for (auto *upper_leaf : leaves) {
			if (upper_leaf->filter_type != TableFilterType::CONSTANT_COMPARISON) {
				continue;
			}
			auto &upper_filter = upper_leaf->Cast<ConstantFilter>();
			if (upper_filter.comparison_type != ExpressionType::COMPARE_LESSTHAN) {
				continue;
			}
			std::string upper;
			if (!H5TreeTryGetStringConstant(upper_filter.constant, upper)) {
				continue;
			}
			if (!H5TreeMatchesPrefixUpperBound(lower, upper)) {
				continue;
			}
			auto normalized = H5TreeNormalizeAbsolutePrefix(lower);
			if (!normalized) {
				continue;
			}
			auto slash = normalized->rfind('/');
			auto fallback_root = slash == std::string::npos || slash == 0 ? "/" : normalized->substr(0, slash);
			if (fallback_root.size() > result.fallback_root.size()) {
				result.fallback_root = std::move(fallback_root);
			}
			if (!result.rooted_prefix || normalized->size() > result.rooted_prefix->size()) {
				result.rooted_prefix = std::move(normalized);
			}
		}
	}
	return result;
}

static H5TreeScanPlan H5TreeBuildPlan(const H5TreeBindData &bind_data, TableFunctionInitInput &input) {
	H5TreeScanPlan plan;
	plan.column_ids = input.column_ids;
	plan.projection_ids = input.projection_ids;
	plan.remove_filter_columns = input.CanRemoveFilterColumns();
	plan.need_projected_attributes.resize(bind_data.projected_attributes.size(), false);

	for (auto column_id : input.column_ids) {
		switch (column_id) {
		case 1:
			plan.need_type = true;
			break;
		case 2:
			plan.need_dtype = true;
			break;
		case 3:
			plan.need_shape = true;
			break;
		default:
			if (column_id >= 4) {
				auto attr_idx = column_id - 4;
				if (attr_idx < plan.need_projected_attributes.size()) {
					plan.need_projected_attributes[attr_idx] = true;
					plan.need_any_projected_attributes = true;
				}
			}
			break;
		}
	}

	if (input.filters) {
		for (const auto &entry : input.filters->filters) {
			auto scan_idx = entry.first;
			auto column_id = input.column_ids[scan_idx];
			if (column_id == 0) {
				plan.path_filter = entry.second->Copy();
			} else if (column_id == 1) {
				plan.type_filter = entry.second->Copy();
				plan.need_type = true;
			}
		}
	}

	if (plan.path_filter) {
		auto exact_path = H5TreeExtractExactPathValue(*plan.path_filter);
		if (exact_path && !exact_path->empty() && (*exact_path)[0] == '/') {
			plan.use_exact_path = true;
			plan.exact_path = *exact_path;
		} else {
			auto hints = H5TreeExtractTraversalHint(*plan.path_filter);
			plan.traversal_root = std::move(hints.fallback_root);
			plan.traversal_root_hint = std::move(hints.rooted_prefix);
		}
	}

	return plan;
}

static void H5TreeWriteOptionalString(Vector &vector, idx_t row_idx, const std::optional<std::string> &value) {
	auto &validity = FlatVector::Validity(vector);
	if (!value) {
		validity.SetInvalid(row_idx);
		return;
	}
	validity.SetValid(row_idx);
	FlatVector::GetData<string_t>(vector)[row_idx] = StringVector::AddString(vector, *value);
}

static void H5TreeWriteShapeRow(Vector &shape_vector, idx_t row_idx, const H5TreeRow &row, idx_t &shape_offset,
                                uint64_t *shape_data) {
	auto entries = ListVector::GetData(shape_vector);
	auto &validity = FlatVector::Validity(shape_vector);
	if (!row.has_shape) {
		validity.SetInvalid(row_idx);
		entries[row_idx].offset = 0;
		entries[row_idx].length = 0;
		return;
	}
	validity.SetValid(row_idx);
	entries[row_idx].offset = shape_offset;
	entries[row_idx].length = row.shape.size();
	for (auto dim : row.shape) {
		shape_data[shape_offset++] = static_cast<uint64_t>(dim);
	}
}

static void H5TreePopulateProjectedAttributeValue(H5TreeProjectedAttributeValue &target, hid_t object_id,
                                                  const H5TreeProjectedAttributeSpec &spec) {
	auto exists = H5Aexists(object_id, spec.attribute_name.c_str());
	if (exists < 0) {
		throw IOException("Failed to inspect attribute: " + spec.attribute_name);
	}
	if (exists == 0) {
		target.present = false;
		return;
	}

	H5AttributeHandle attr(object_id, spec.attribute_name.c_str());
	if (!attr.is_valid()) {
		throw IOException("Failed to open attribute: " + spec.attribute_name);
	}
	hid_t type_id = H5Aget_type(attr);
	if (type_id < 0) {
		throw IOException("Failed to get type for attribute: " + spec.attribute_name);
	}
	H5TypeHandle type = H5TypeHandle::TakeOwnershipOf(type_id);
	hid_t space_id = H5Aget_space(attr);
	if (space_id < 0) {
		throw IOException("Failed to get dataspace for attribute: " + spec.attribute_name);
	}
	H5DataspaceHandle space = H5DataspaceHandle::TakeOwnershipOf(space_id);

	auto source_type = H5ResolveAttributeLogicalType(type.get(), space.get(), spec.attribute_name);
	auto value = H5ReadAttributeValue(attr, type.get(), source_type, spec.attribute_name);
	Value cast_value;
	string error_message;
	if (!value.DefaultTryCastAs(spec.output_type, cast_value, &error_message, false)) {
		throw IOException("Attribute '" + spec.attribute_name + "' contains values that cannot be cast to " +
		                  spec.output_type.ToString());
	}
	target.present = true;
	target.value = std::move(cast_value);
}

class H5TreeScanner {
public:
	H5TreeScanner(ClientContext &context_p, const H5TreeBindData &bind_data_p, const H5TreeScanPlan &plan_p)
	    : context(context_p), bind_data(bind_data_p), plan(plan_p) {
		H5ErrorSuppressor suppress;
		file = H5FileHandle(&context, bind_data.filename.c_str(), H5F_ACC_RDONLY, bind_data.swmr);
		if (!file.is_valid()) {
			throw IOException(FormatRemoteFileError("Failed to open HDF5 file", bind_data.filename));
		}
		std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
		InitializeRootIdentity();
		root_ancestry = std::make_shared<H5TreeAncestryNode>(H5TreeAncestryNode {root_identity, nullptr});
		traversal_root = DetermineTraversalRoot();
	}

	~H5TreeScanner() {
		RequestStop();
		JoinWorker();
	}

	void ReadRows(idx_t max_rows, vector<H5TreeRow> &rows) {
		using namespace std::chrono_literals;
		StartWorkerIfNeeded();
		rows.clear();
		rows.reserve(max_rows);
		while (rows.size() < max_rows) {
			ThrowIfInterrupted(context);
			std::unique_lock<std::mutex> lock(queue_lock);
			while (row_queue.empty() && !stop_requested && !worker_done && !worker_failed) {
				if (queue_not_empty.wait_for(lock, 200ms, [&] {
					    return !row_queue.empty() || stop_requested || worker_done || worker_failed;
				    })) {
					break;
				}
				lock.unlock();
				ThrowIfInterrupted(context);
				lock.lock();
			}
			if (worker_failed) {
				auto message = worker_error;
				lock.unlock();
				JoinWorker();
				throw IOException(FormatRemoteFileError(message, bind_data.filename));
			}
			if (row_queue.empty()) {
				break;
			}
			while (rows.size() < max_rows && !row_queue.empty()) {
				rows.push_back(std::move(row_queue.front()));
				row_queue.pop_front();
			}
			lock.unlock();
		}
	}

private:
	ClientContext &context;
	const H5TreeBindData &bind_data;
	const H5TreeScanPlan &plan;
	H5FileHandle file;
	H5TreeObjectIdentity root_identity;
	std::shared_ptr<const H5TreeAncestryNode> root_ancestry;
	std::unordered_map<H5TreeObjectIdentity, H5TreeCachedObject, H5TreeObjectIdentityHash> object_cache;
	std::string traversal_root = "/";

	std::mutex queue_lock;
	std::condition_variable queue_not_empty;
	std::deque<H5TreeRow> row_queue;
	std::thread worker_thread;
	std::atomic<bool> stop_requested {false};
	bool worker_started = false;
	bool worker_done = false;
	bool worker_failed = false;
	std::string worker_error;

	void InitializeRootIdentity() {
		H5O_info2_t root_info;
		if (H5Oget_info3(file, &root_info, H5O_INFO_BASIC) < 0) {
			throw IOException(FormatRemoteFileError("Failed to inspect HDF5 root object", bind_data.filename));
		}
		root_identity = H5TreeIdentityFromObjectInfo(root_info);
		auto &cached_root = object_cache[root_identity];
		cached_root.type = H5TreeEntryType::GROUP;
		cached_root.type_loaded = true;
	}

	void StartWorkerIfNeeded() {
		std::lock_guard<std::mutex> lock(queue_lock);
		if (worker_started) {
			return;
		}
		worker_started = true;
		worker_thread = std::thread([this] { WorkerMain(); });
	}

	void RequestStop() {
		stop_requested = true;
		queue_not_empty.notify_all();
	}

	void JoinWorker() {
		if (worker_thread.joinable()) {
			worker_thread.join();
		}
	}

	void MarkDone() {
		std::lock_guard<std::mutex> lock(queue_lock);
		worker_done = true;
		queue_not_empty.notify_all();
	}

	void MarkFailed(const std::string &message) {
		std::lock_guard<std::mutex> lock(queue_lock);
		stop_requested = true;
		worker_done = true;
		worker_failed = true;
		worker_error = H5TreeNormalizeExceptionMessage(message);
		queue_not_empty.notify_all();
	}

	void WorkerMain() {
		try {
			if (plan.use_exact_path) {
				RunExactPath();
			} else {
				RunTraversal();
			}
			MarkDone();
		} catch (const std::exception &ex) {
			MarkFailed(ex.what());
		}
	}

	void RunExactPath() {
		std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
		if (plan.exact_path == "/") {
			ProduceRootRow();
			return;
		}
		auto walk = WalkAbsolutePath(plan.exact_path, nullptr, false);
		if (!walk.found) {
			return;
		}
		auto resolved = ResolveEntry(plan.exact_path, walk.final_link_info, walk.parent_ancestry, -1, nullptr, true);
		ProduceRow(plan.exact_path, resolved, -1, nullptr);
	}

	void RunTraversal() {
		std::deque<H5TreeVisitTask> tasks;
		{
			std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
			if (traversal_root == "/") {
				ProduceRootRow();
				tasks.push_back(H5TreeVisitTask {"/", root_ancestry});
			} else {
				auto walk = WalkAbsolutePath(traversal_root, nullptr, false);
				if (!walk.found) {
					return;
				}
				auto resolved =
				    ResolveEntry(traversal_root, walk.final_link_info, walk.parent_ancestry, -1, nullptr, true);
				ProduceRow(traversal_root, resolved, -1, nullptr);
				if (resolved.traversable_group && !resolved.cycle) {
					tasks.push_back(H5TreeVisitTask {traversal_root, resolved.ancestry});
				}
			}
		}

		while (!stop_requested && !tasks.empty()) {
			auto task = std::move(tasks.front());
			tasks.pop_front();
			VisitTask(task, tasks);
		}
	}

	void ProduceRootRow() {
		H5TreeRow row;
		row.path = "/";
		row.type = H5TreeTypeName(H5TreeEntryType::GROUP);
		row.projected_values.resize(bind_data.projected_attributes.size());
		if (!PathMatches(row.path) || !TypeMatches(row.type)) {
			return;
		}
		PopulateProjectedAttributes(row, H5TreeEntryType::GROUP, root_identity, "/", -1, nullptr);
		EnqueueRow(std::move(row));
	}

	void VisitTask(const H5TreeVisitTask &task, std::deque<H5TreeVisitTask> &tasks) {
		H5TreeVisitContext visit_context;
		visit_context.scanner = this;
		visit_context.root_path = task.root_path;
		visit_context.root_ancestry = task.ancestry;
		visit_context.group_ancestry.emplace(task.root_path, task.ancestry);

		std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
		H5ErrorSuppressor suppress;
		auto status = task.root_path == "/"
		                  ? H5Lvisit2(file, H5_INDEX_NAME, H5_ITER_INC, VisitCallback, &visit_context)
		                  : H5Lvisit_by_name2(file, task.root_path.c_str(), H5_INDEX_NAME, H5_ITER_INC, VisitCallback,
		                                      &visit_context, H5P_DEFAULT);
		if (status < 0 && !stop_requested) {
			throw IOException(task.root_path == "/" ? "Failed to traverse HDF5 namespace"
			                                        : "Failed to traverse HDF5 namespace subtree");
		}
		for (auto &deferred : visit_context.deferred_tasks) {
			tasks.push_back(std::move(deferred));
		}
	}

	std::string DetermineTraversalRoot() {
		if (plan.traversal_root_hint) {
			auto walk = WalkAbsolutePath(*plan.traversal_root_hint, nullptr, false);
			if (walk.found) {
				return *plan.traversal_root_hint;
			}
		}
		return plan.traversal_root;
	}

	H5TreePathWalkResult
	WalkAbsolutePath(const std::string &path,
	                 std::unordered_map<std::string, std::shared_ptr<const H5TreeAncestryNode>> *ancestry_cache,
	                 bool require_final_group) {
		if (path.empty() || path[0] != '/') {
			return {};
		}

		auto parts = H5TreeSplitAbsolutePath(path);
		if (parts.empty()) {
			H5TreePathWalkResult result;
			result.found = true;
			result.parent_ancestry = nullptr;
			result.final_ancestry = root_ancestry;
			return result;
		}

		auto current_ancestry = root_ancestry;
		hid_t current_loc = file;
		unique_ptr<H5ObjectHandle> current_group;
		std::string current_path;

		H5ErrorSuppressor suppress;
		for (idx_t i = 0; i < parts.size(); i++) {
			const auto &part = parts[i];
			auto is_final = i + 1 == parts.size();

			H5L_info2_t link_info;
			if (H5Lget_info2(current_loc, part.c_str(), &link_info, H5P_DEFAULT) < 0) {
				return {};
			}
			current_path += "/" + part;

			if (is_final) {
				H5TreePathWalkResult result;
				result.found = true;
				result.final_link_info = link_info;
				result.parent_ancestry = current_ancestry;
				if (require_final_group) {
					auto resolved =
					    ResolveEntry(current_path, link_info, current_ancestry, current_loc, part.c_str(), true);
					if (!resolved.traversable_group || !resolved.ancestry) {
						return {};
					}
					if (ancestry_cache) {
						(*ancestry_cache)[current_path] = resolved.ancestry;
					}
					result.final_ancestry = resolved.ancestry;
				}
				return result;
			}

			if (link_info.type == H5L_TYPE_EXTERNAL) {
				return {};
			}
			bool resolved_from_cache = false;
			if (ancestry_cache) {
				auto cached = ancestry_cache->find(current_path);
				if (cached != ancestry_cache->end()) {
					current_ancestry = cached->second;
					resolved_from_cache = true;
				}
			}
			if (!resolved_from_cache) {
				auto resolved =
				    ResolveEntry(current_path, link_info, current_ancestry, current_loc, part.c_str(), true);
				if (!resolved.traversable_group || !resolved.ancestry || resolved.cycle) {
					return {};
				}
				current_ancestry = resolved.ancestry;
				if (ancestry_cache) {
					(*ancestry_cache)[current_path] = current_ancestry;
				}
			}
			auto next_group = make_uniq<H5ObjectHandle>(current_loc, part.c_str());
			if (!next_group->is_valid()) {
				return {};
			}
			current_loc = next_group->get();
			current_group = std::move(next_group);
		}

		return {};
	}

	std::shared_ptr<const H5TreeAncestryNode> EnsureGroupAncestry(H5TreeVisitContext &visit_context,
	                                                              const std::string &path) {
		auto walk = WalkAbsolutePath(path, &visit_context.group_ancestry, true);
		if (!walk.found || !walk.final_ancestry) {
			throw IOException("Expected group path during tree traversal: " + path);
		}
		return walk.final_ancestry;
	}

	void EnqueueRow(H5TreeRow row) {
		std::lock_guard<std::mutex> lock(queue_lock);
		if (stop_requested) {
			return;
		}
		row_queue.push_back(std::move(row));
		queue_not_empty.notify_one();
	}

	bool PathMatches(const std::string &path) const {
		return !plan.path_filter || H5TreeEvaluateStringFilter(*plan.path_filter, std::optional<std::string>(path));
	}

	bool TypeMatches(const std::optional<std::string> &type_name) const {
		return !plan.type_filter || H5TreeEvaluateStringFilter(*plan.type_filter, type_name);
	}

	H5ObjectHandle OpenObjectByIdentity(const H5TreeObjectIdentity &identity, const std::string &path) {
		if (identity.fileno == root_identity.fileno) {
			H5O_token_t token;
			std::memcpy(&token, identity.token.data(), sizeof(H5O_token_t));
			H5ErrorSuppressor suppress;
			auto object_id = H5Oopen_by_token(file, token);
			if (object_id >= 0) {
				return H5ObjectHandle::TakeOwnershipOf(object_id);
			}
		}
		return H5ObjectHandle(file, path.c_str());
	}

	H5ObjectHandle OpenObject(const H5TreeObjectIdentity &identity, const std::string &path, hid_t parent_loc,
	                          const char *link_name) {
		if (parent_loc >= 0 && link_name) {
			return H5ObjectHandle(parent_loc, link_name);
		}
		return OpenObjectByIdentity(identity, path);
	}

	bool ResolveObjectInfo(H5O_info2_t &info, const std::optional<H5TreeObjectIdentity> &identity,
	                       const std::string &path, hid_t parent_loc, const char *link_name) {
		H5ErrorSuppressor suppress;
		if (parent_loc >= 0 && link_name) {
			return H5Oget_info_by_name3(parent_loc, link_name, &info, H5O_INFO_BASIC, H5P_DEFAULT) >= 0;
		}
		if (identity) {
			auto object = OpenObjectByIdentity(*identity, path);
			return object.is_valid() && H5Oget_info3(object, &info, H5O_INFO_BASIC) >= 0;
		}
		return H5Oget_info_by_name3(file, path.c_str(), &info, H5O_INFO_BASIC, H5P_DEFAULT) >= 0;
	}

	static H5TreeEntryType EntryTypeFromObjectInfo(const H5O_info2_t &info) {
		switch (info.type) {
		case H5O_TYPE_GROUP:
			return H5TreeEntryType::GROUP;
		case H5O_TYPE_DATASET:
			return H5TreeEntryType::DATASET;
		case H5O_TYPE_NAMED_DATATYPE:
			return H5TreeEntryType::DATATYPE;
		default:
			return H5TreeEntryType::UNKNOWN;
		}
	}

	H5TreeResolvedEntry ResolveEntry(const std::string &path, const H5L_info2_t &link_info,
	                                 const std::shared_ptr<const H5TreeAncestryNode> &parent_ancestry, hid_t parent_loc,
	                                 const char *link_name, bool force_resolution) {
		H5TreeResolvedEntry result;
		result.is_soft_link = link_info.type == H5L_TYPE_SOFT;

		if (link_info.type == H5L_TYPE_EXTERNAL) {
			result.type_kind = H5TreeEntryType::EXTERNAL;
			return result;
		}

		if (link_info.type == H5L_TYPE_HARD) {
			auto identity = H5TreeIdentityFromToken(root_identity.fileno, link_info.u.token);
			auto cache_it = object_cache.find(identity);
			result.identity = identity;
			if (cache_it != object_cache.end() && cache_it->second.type_loaded) {
				result.type_kind = cache_it->second.type;
			}
		}

		auto need_resolution = force_resolution || result.is_soft_link || link_info.type != H5L_TYPE_HARD ||
		                       plan.need_type || plan.need_dtype || plan.need_shape ||
		                       plan.need_any_projected_attributes;
		if (need_resolution && (!result.identity || result.type_kind == H5TreeEntryType::UNKNOWN)) {
			H5O_info2_t info;
			if (!ResolveObjectInfo(info, result.identity, path, parent_loc, link_name)) {
				result.type_kind = H5TreeEntryType::LINK;
				result.identity.reset();
				return result;
			}

			result.identity = H5TreeIdentityFromObjectInfo(info);
			result.type_kind = EntryTypeFromObjectInfo(info);

			auto &cached = object_cache[*result.identity];
			cached.type = result.type_kind;
			cached.type_loaded = true;
		}

		if (result.type_kind == H5TreeEntryType::GROUP && result.identity) {
			result.traversable_group = true;
			result.cycle = parent_ancestry && parent_ancestry->Contains(*result.identity);
			result.ancestry =
			    std::make_shared<H5TreeAncestryNode>(H5TreeAncestryNode {*result.identity, parent_ancestry});
		}

		return result;
	}

	void PopulateDatasetMetadata(H5TreeRow &row, H5TreeEntryType type_kind, const H5TreeObjectIdentity &identity,
	                             const std::string &path, hid_t parent_loc, const char *link_name) {
		if (!plan.need_dtype && !plan.need_shape) {
			return;
		}
		if (type_kind != H5TreeEntryType::DATASET) {
			return;
		}
		auto &cached = object_cache[identity];
		if (!cached.dtype_loaded || !cached.shape_loaded) {
			H5ErrorSuppressor suppress;
			auto dataset = OpenObject(identity, path, parent_loc, link_name);
			if (!dataset.is_valid()) {
				throw IOException("Failed to open dataset during tree traversal: " + path);
			}
			if (!cached.dtype_loaded) {
				hid_t type_id = H5Dget_type(dataset);
				if (type_id < 0) {
					throw IOException("Failed to get dataset type during tree traversal: " + path);
				}
				H5TypeHandle type = H5TypeHandle::TakeOwnershipOf(type_id);
				cached.dtype = H5TypeToString(type);
				cached.dtype_loaded = true;
			}
			if (!cached.shape_loaded) {
				cached.shape = H5GetShape(dataset);
				cached.shape_loaded = true;
			}
		}
		if (plan.need_dtype) {
			row.dtype = cached.dtype;
		}
		if (plan.need_shape) {
			row.shape = cached.shape;
			row.has_shape = true;
		}
	}

	void PopulateProjectedAttributes(H5TreeRow &row, H5TreeEntryType type_kind, const H5TreeObjectIdentity &identity,
	                                 const std::string &path, hid_t parent_loc, const char *link_name) {
		if (!plan.need_any_projected_attributes || !H5TreeCanHaveProjectedAttributes(type_kind)) {
			return;
		}
		auto &cached = object_cache[identity];
		if (cached.projected_values.size() != bind_data.projected_attributes.size()) {
			cached.projected_values.resize(bind_data.projected_attributes.size());
			cached.projected_loaded.assign(bind_data.projected_attributes.size(), false);
		}
		std::vector<idx_t> missing;
		for (idx_t i = 0; i < plan.need_projected_attributes.size(); i++) {
			if (plan.need_projected_attributes[i] && !cached.projected_loaded[i]) {
				missing.push_back(i);
			}
		}
		if (!missing.empty()) {
			H5ErrorSuppressor suppress;
			auto object = OpenObject(identity, path, parent_loc, link_name);
			if (!object.is_valid()) {
				throw IOException("Failed to open object during tree traversal: " + path);
			}
			for (auto attr_idx : missing) {
				H5TreePopulateProjectedAttributeValue(cached.projected_values[attr_idx], object,
				                                      bind_data.projected_attributes[attr_idx]);
				cached.projected_loaded[attr_idx] = true;
			}
		}
		for (idx_t i = 0; i < plan.need_projected_attributes.size(); i++) {
			if (plan.need_projected_attributes[i]) {
				row.projected_values[i] = cached.projected_values[i];
			}
		}
	}

	void ProduceRow(const std::string &path, const H5TreeResolvedEntry &resolved, hid_t parent_loc,
	                const char *link_name) {
		H5TreeRow row;
		row.path = path;
		row.type = H5TreeTypeName(resolved.type_kind);
		row.projected_values.resize(bind_data.projected_attributes.size());
		if (!PathMatches(path) || !TypeMatches(row.type)) {
			return;
		}
		if (resolved.identity) {
			PopulateDatasetMetadata(row, resolved.type_kind, *resolved.identity, path, parent_loc, link_name);
			PopulateProjectedAttributes(row, resolved.type_kind, *resolved.identity, path, parent_loc, link_name);
		}
		EnqueueRow(std::move(row));
	}

	static herr_t VisitCallback(hid_t parent_loc, const char *name, const H5L_info2_t *info, void *op_data) {
		auto &visit_context = *reinterpret_cast<H5TreeVisitContext *>(op_data);
		auto &scanner = *visit_context.scanner;
		try {
			if (scanner.stop_requested) {
				return 1;
			}
			auto full_path = H5TreeDeriveVisitedPath(visit_context.root_path, name);
			auto parent_path = H5TreeParentPath(full_path);
			auto ancestry_it = visit_context.group_ancestry.find(parent_path);
			std::shared_ptr<const H5TreeAncestryNode> parent_ancestry;
			if (ancestry_it != visit_context.group_ancestry.end()) {
				parent_ancestry = ancestry_it->second;
			} else if (info->type == H5L_TYPE_SOFT && parent_path != "/") {
				parent_ancestry = scanner.EnsureGroupAncestry(visit_context, parent_path);
			} else {
				parent_ancestry = visit_context.root_ancestry;
			}
			auto resolved = scanner.ResolveEntry(full_path, *info, parent_ancestry, parent_loc, name, false);
			scanner.ProduceRow(full_path, resolved, parent_loc, name);
			if (resolved.traversable_group && resolved.ancestry) {
				visit_context.group_ancestry[full_path] = resolved.ancestry;
				if (resolved.is_soft_link && !resolved.cycle) {
					visit_context.deferred_tasks.push_back(H5TreeVisitTask {full_path, resolved.ancestry});
				}
			}
			return scanner.stop_requested ? 1 : 0;
		} catch (const std::exception &ex) {
			scanner.MarkFailed(ex.what());
			return -1;
		}
	}
};

static bool H5TreeSupportsPushdownType(const FunctionData &, idx_t column_index) {
	return column_index == 0 || column_index == 1;
}

static unique_ptr<FunctionData> H5TreeBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<H5TreeBindData>();
	if (input.inputs.empty()) {
		throw InvalidInputException("h5_tree requires at least 1 argument: filename");
	}
	result->filename = input.inputs[0].GetValue<string>();
	result->swmr = ResolveSwmrOption(context, input.named_parameters);

	names = {"path", "type", "dtype", "shape"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::LIST(LogicalType::UBIGINT)};
	for (idx_t i = 1; i < input.inputs.size(); i++) {
		auto spec = H5TreeParseProjectedAttributeSpec(input.inputs[i]);
		names.push_back(spec.output_column_name);
		return_types.push_back(spec.output_type);
		result->projected_attributes.push_back(std::move(spec));
	}
	result->all_return_types = return_types;
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> H5TreeInit(ClientContext &context, TableFunctionInitInput &input) {
	ThrowIfInterrupted(context);
	auto &bind_data = input.bind_data->Cast<H5TreeBindData>();
	auto result = make_uniq<H5TreeGlobalState>();
	result->plan = H5TreeBuildPlan(bind_data, input);
	result->scanner = make_uniq<H5TreeScanner>(context, bind_data, result->plan);
	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> H5TreeInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                           GlobalTableFunctionState *global_state_p) {
	auto result = make_uniq<H5TreeLocalState>();
	if (input.CanRemoveFilterColumns()) {
		vector<LogicalType> scanned_types;
		auto &bind_data = input.bind_data->Cast<H5TreeBindData>();
		scanned_types.reserve(input.column_ids.size());
		for (auto column_id : input.column_ids) {
			scanned_types.push_back(bind_data.all_return_types[column_id]);
		}
		result->all_columns.Initialize(context.client, scanned_types);
	}
	return std::move(result);
}

static void H5TreeWriteRow(const H5TreeRow &row, const H5TreeBindData &bind_data, const vector<column_t> &column_ids,
                           DataChunk &chunk, idx_t row_idx, idx_t &shape_offset, uint64_t *shape_data) {
	for (idx_t column_idx = 0; column_idx < column_ids.size(); column_idx++) {
		auto column_id = column_ids[column_idx];
		auto &vector = chunk.data[column_idx];
		switch (column_id) {
		case 0:
			FlatVector::GetData<string_t>(vector)[row_idx] = StringVector::AddString(vector, row.path);
			break;
		case 1:
			H5TreeWriteOptionalString(vector, row_idx, row.type);
			break;
		case 2:
			H5TreeWriteOptionalString(vector, row_idx, row.dtype);
			break;
		case 3:
			H5TreeWriteShapeRow(vector, row_idx, row, shape_offset, shape_data);
			break;
		default: {
			auto attr_idx = column_id - 4;
			const auto &projected = row.projected_values[attr_idx];
			if (projected.present) {
				vector.SetValue(row_idx, projected.value);
			} else {
				vector.SetValue(row_idx, bind_data.projected_attributes[attr_idx].default_value);
			}
			break;
		}
		}
	}
}

static void H5TreeScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	ThrowIfInterrupted(context);
	auto &bind_data = data.bind_data->Cast<H5TreeBindData>();
	auto &gstate = data.global_state->Cast<H5TreeGlobalState>();
	auto remove_filter_columns = gstate.plan.remove_filter_columns;
	vector<H5TreeRow> rows;
	gstate.scanner->ReadRows(STANDARD_VECTOR_SIZE, rows);
	if (rows.empty()) {
		output.SetCardinality(0);
		return;
	}

	DataChunk *target_chunk = &output;
	if (remove_filter_columns) {
		auto &lstate = data.local_state->Cast<H5TreeLocalState>();
		lstate.all_columns.Reset();
		target_chunk = &lstate.all_columns;
	}
	target_chunk->SetCardinality(rows.size());

	idx_t shape_column_idx = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < gstate.plan.column_ids.size(); i++) {
		if (gstate.plan.column_ids[i] == 3) {
			shape_column_idx = i;
			break;
		}
	}

	idx_t total_shape_elems = 0;
	Vector *shape_vector = nullptr;
	uint64_t *shape_data = nullptr;
	idx_t shape_offset = 0;
	if (shape_column_idx != DConstants::INVALID_INDEX) {
		for (auto &row : rows) {
			if (row.has_shape) {
				total_shape_elems += row.shape.size();
			}
		}
		shape_vector = &target_chunk->data[shape_column_idx];
	}
	if (shape_vector) {
		ListVector::Reserve(*shape_vector, total_shape_elems);
		auto &child = ListVector::GetEntry(*shape_vector);
		shape_data = FlatVector::GetData<uint64_t>(child);
	}

	for (idx_t row_idx = 0; row_idx < rows.size(); row_idx++) {
		H5TreeWriteRow(rows[row_idx], bind_data, gstate.plan.column_ids, *target_chunk, row_idx, shape_offset,
		               shape_data);
	}
	if (shape_vector) {
		ListVector::SetListSize(*shape_vector, shape_offset);
	}
	if (remove_filter_columns) {
		output.ReferenceColumns(data.local_state->Cast<H5TreeLocalState>().all_columns, gstate.plan.projection_ids);
	} else {
		output.SetCardinality(rows.size());
	}
}

static void H5AttrFunction(DataChunk &args, ExpressionState &, Vector &result) {
	auto &name_vec = args.data[0];
	auto &default_vec = args.data[1];
	auto &children = StructVector::GetEntries(result);
	D_ASSERT(children.size() == 3);
	auto &tag_child = GetTreeStructChild(children[0]);
	auto &name_child = GetTreeStructChild(children[1]);
	auto &default_child = GetTreeStructChild(children[2]);
	default_child.Reference(default_vec);

	UnifiedVectorFormat name_data;
	name_vec.ToUnifiedFormat(args.size(), name_data);
	auto name_ptr = UnifiedVectorFormat::GetData<string_t>(name_data);
	for (idx_t i = 0; i < args.size(); i++) {
		auto name_idx = name_data.sel->get_index(i);
		if (!name_data.validity.RowIsValid(name_idx)) {
			throw InvalidInputException("h5_attr name must not be NULL");
		}
		FlatVector::GetData<string_t>(tag_child)[i] = StringVector::AddString(tag_child, "__attr__");
		FlatVector::GetData<string_t>(name_child)[i] = StringVector::AddString(name_child, name_ptr[name_idx]);
	}

	bool all_const = default_vec.GetVectorType() == VectorType::CONSTANT_VECTOR &&
	                 name_vec.GetVectorType() == VectorType::CONSTANT_VECTOR;
	result.SetVectorType(all_const ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
	result.Verify(args.size());
}

static unique_ptr<FunctionData> H5AttrBind(ClientContext &, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 2) {
		throw InvalidInputException("h5_attr() requires two arguments: attribute name and default value");
	}
	if (!arguments[1]->IsFoldable()) {
		throw InvalidInputException("h5_attr default_value must be a constant expression");
	}
	if (arguments[1]->return_type.id() == LogicalTypeId::SQLNULL) {
		throw InvalidInputException(
		    "h5_attr default_value must have a concrete type; use an explicit cast such as NULL::VARCHAR");
	}
	child_list_t<LogicalType> struct_children = {{"tag", LogicalType::VARCHAR},
	                                             {"attribute_name", arguments[0]->return_type},
	                                             {"default_value", arguments[1]->return_type}};
	bound_function.return_type = LogicalType::STRUCT(struct_children);
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

void RegisterH5AttrFunction(ExtensionLoader &loader) {
	ScalarFunction h5_attr("h5_attr", {LogicalType::VARCHAR, LogicalType::ANY}, LogicalTypeId::STRUCT, H5AttrFunction,
	                       H5AttrBind);
	h5_attr.serialize = VariableReturnBindData::Serialize;
	h5_attr.deserialize = VariableReturnBindData::Deserialize;
	h5_attr.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	loader.RegisterFunction(h5_attr);
}

void RegisterH5TreeFunction(ExtensionLoader &loader) {
	TableFunction h5_tree("h5_tree", {LogicalType::VARCHAR}, H5TreeScan, H5TreeBind, H5TreeInit, H5TreeInitLocal);
	h5_tree.varargs = LogicalType::ANY;
	h5_tree.named_parameters["swmr"] = LogicalType::BOOLEAN;
	h5_tree.projection_pushdown = true;
	h5_tree.filter_pushdown = true;
	h5_tree.filter_prune = true;
	h5_tree.supports_pushdown_type = H5TreeSupportsPushdownType;
	loader.RegisterFunction(h5_tree);
}

} // namespace duckdb
