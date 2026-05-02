#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "h5_tree_shared.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#if __has_include("duckdb/common/vector/flat_vector.hpp")
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#else
#include "duckdb/common/types/vector.hpp"
#endif
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
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

using H5TreeAncestorSet = std::set<H5TreeObjectIdentity>;

struct H5TreeBindData : public TableFunctionData {
	vector<string> filenames;
	vector<H5TreeProjectedAttributeSpec> projected_attributes;
	bool swmr = false;
	std::optional<idx_t> visible_filename_idx;
	bool had_glob = false;

	bool SupportStatementCache() const override {
		return !had_glob;
	}
};

struct H5TreeOutputColumn {
	idx_t output_idx;
	column_t column_id;
};

struct H5TreeScanLayout {
	vector<H5TreeOutputColumn> projected_columns;
	vector<idx_t> filename_output_idxs;
	std::optional<idx_t> shape_output_idx;
};

struct H5TreeGlobalState : public GlobalTableFunctionState {
	unique_ptr<class H5TreeScanner> scanner;
	idx_t file_idx = 0;
	H5TreeScanLayout output_layout;

	idx_t MaxThreads() const override {
		return 1;
	}
};

struct H5TreeGroupStackEntry {
	std::string path;
	H5TreeObjectIdentity identity;
};

struct H5TreeVisitTask {
	std::string root_path;
	H5TreeObjectIdentity root_identity;
	H5TreeAncestorSet ancestor_set;
};

class H5TreeScanner;

struct H5TreeVisitContext {
	H5TreeScanner *scanner;
	std::string root_path;
	const H5TreeAncestorSet *root_ancestor_set = nullptr;
	std::vector<H5TreeGroupStackEntry> group_stack;
	H5TreeAncestorSet expanded_group_identities;
	std::deque<H5TreeVisitTask> deferred_tasks;
};

static bool H5TreeOutputHasColumnName(const vector<string> &names, const string &column_name) {
	return std::any_of(names.begin(), names.end(),
	                   [&](const string &name) { return StringUtil::CIEquals(name, column_name); });
}

static virtual_column_map_t H5GetFilenameVirtualColumns(ClientContext &, optional_ptr<FunctionData> bind_data_p) {
	virtual_column_map_t result;
	if (bind_data_p && bind_data_p->Cast<H5TreeBindData>().visible_filename_idx.has_value()) {
		return result;
	}
	result.emplace(MultiFileReader::COLUMN_IDENTIFIER_FILENAME, TableColumn("filename", LogicalType::VARCHAR));
	return result;
}

static bool H5TreeIsFilenameColumn(const H5TreeBindData &bind_data, column_t column_id) {
	return column_id == MultiFileReader::COLUMN_IDENTIFIER_FILENAME ||
	       (bind_data.visible_filename_idx.has_value() && column_id == *bind_data.visible_filename_idx);
}

static void H5TreePopulateFilenameColumns(const string &filename, const vector<idx_t> &filename_output_idxs,
                                          DataChunk &output) {
	if (output.size() == 0) {
		return;
	}
	for (auto output_idx : filename_output_idxs) {
		auto &vector = output.data[output_idx];
		vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::GetData<string_t>(vector)[0] = StringVector::AddString(vector, filename);
	}
}

static H5TreeScanLayout H5TreeBuildOutputLayout(const H5TreeBindData &bind_data, const vector<column_t> &column_ids) {
	H5TreeScanLayout result;
	for (idx_t output_idx = 0; output_idx < column_ids.size(); output_idx++) {
		auto column_id = column_ids[output_idx];
		if (H5TreeIsFilenameColumn(bind_data, column_id)) {
			result.filename_output_idxs.push_back(output_idx);
			continue;
		}
		if (column_id == 3) {
			result.shape_output_idx = output_idx;
		}
		result.projected_columns.push_back({output_idx, column_id});
	}
	return result;
}

static bool H5TreeAncestorSetContains(const H5TreeAncestorSet &ancestors, const H5TreeObjectIdentity &identity) {
	return ancestors.find(identity) != ancestors.end();
}

static H5TreeAncestorSet H5TreeBuildDeferredAncestorSet(const H5TreeAncestorSet &root_ancestors,
                                                        const std::vector<H5TreeGroupStackEntry> &group_stack) {
	H5TreeAncestorSet result = root_ancestors;
	for (const auto &entry : group_stack) {
		result.insert(entry.identity);
	}
	return result;
}

static void H5TreeMaybeDeferGroupTraversal(H5TreeVisitContext &visit_context, const std::string &path,
                                           const H5TreeObjectIdentity &identity) {
	auto deferred_ancestors =
	    H5TreeBuildDeferredAncestorSet(*visit_context.root_ancestor_set, visit_context.group_stack);
	if (H5TreeAncestorSetContains(deferred_ancestors, identity)) {
		return;
	}
	visit_context.deferred_tasks.push_back(H5TreeVisitTask {path, identity, std::move(deferred_ancestors)});
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

static std::string H5TreeDeriveVisitedPath(const std::string &root_path, const char *name) {
	if (!name || !name[0] || (name[0] == '.' && !name[1])) {
		return root_path;
	}
	if (root_path == "/") {
		return "/" + std::string(name);
	}
	return root_path + "/" + name;
}

class H5TreeScanner {
public:
	H5TreeScanner(ClientContext &context_p, const string &filename_p, bool swmr_p,
	              const vector<H5TreeProjectedAttributeSpec> &projected_attributes_p)
	    : context(context_p), filename(filename_p), projected_attributes(projected_attributes_p),
	      reader(context_p, filename_p, swmr_p, projected_attributes_p) {
		buffered_rows.reserve(STANDARD_VECTOR_SIZE);
	}

	~H5TreeScanner() {
		RequestStop();
		JoinWorker();
	}

	void ReadRows(vector<H5TreeRow> &rows) {
		using namespace std::chrono_literals;
		StartWorkerIfNeeded();
		rows.clear();
		ThrowIfInterrupted(context);
		std::unique_lock<std::mutex> lock(state_lock);
		while (pending_rows.empty() && !stop_requested && worker_state == WorkerState::Running) {
			if (state_cv.wait_for(lock, 200ms, [&] {
				    return !pending_rows.empty() || stop_requested || worker_state != WorkerState::Running;
			    })) {
				break;
			}
			lock.unlock();
			ThrowIfInterrupted(context);
			lock.lock();
		}
		if (worker_state == WorkerState::Failed) {
			auto message = worker_error;
			lock.unlock();
			JoinWorker();
			throw IOException(AppendRemoteError(message, filename));
		}
		if (pending_rows.empty()) {
			return;
		}
		rows = std::move(pending_rows);
		pending_rows.clear();
		lock.unlock();
		state_cv.notify_one();
	}

private:
	enum class WorkerState : uint8_t { NotStarted, Running, Done, Failed };

	ClientContext &context;
	const string &filename;
	const vector<H5TreeProjectedAttributeSpec> &projected_attributes;
	H5TreeFileReader reader;

	std::mutex state_lock;
	std::condition_variable state_cv;
	std::vector<H5TreeRow> pending_rows;
	std::vector<H5TreeRow> buffered_rows;
	std::thread worker_thread;
	std::atomic<bool> stop_requested {false};
	WorkerState worker_state = WorkerState::NotStarted;
	std::string worker_error;

	void StartWorkerIfNeeded() {
		std::lock_guard<std::mutex> lock(state_lock);
		if (worker_state != WorkerState::NotStarted) {
			return;
		}
		worker_state = WorkerState::Running;
		worker_thread = std::thread([this] { WorkerMain(); });
	}

	void RequestStop() {
		stop_requested = true;
		state_cv.notify_all();
	}

	void JoinWorker() {
		if (worker_thread.joinable()) {
			worker_thread.join();
		}
	}

	void MarkDone() {
		std::lock_guard<std::mutex> lock(state_lock);
		if (worker_state == WorkerState::Running) {
			worker_state = WorkerState::Done;
		}
		state_cv.notify_all();
	}

	void MarkFailed(const std::string &message) {
		std::lock_guard<std::mutex> lock(state_lock);
		stop_requested = true;
		worker_state = WorkerState::Failed;
		worker_error = H5NormalizeExceptionMessage(message);
		state_cv.notify_all();
	}

	void WorkerMain() {
		try {
			RunTraversal();
			FlushBufferedRows();
			MarkDone();
		} catch (const std::exception &ex) {
			MarkFailed(ex.what());
		}
	}

	void RunTraversal() {
		std::deque<H5TreeVisitTask> tasks;
		{
			std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
			ProduceRootRow();
			tasks.push_back(H5TreeVisitTask {"/", reader.GetRootIdentity(), H5TreeAncestorSet()});
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
		row.projected_values.resize(projected_attributes.size());
		reader.PopulateRowMetadataAndAttributes(row, H5TreeEntryType::GROUP, reader.GetRootIdentity(), "/", -1,
		                                        nullptr);
		BufferRow(std::move(row));
	}

	void VisitTask(const H5TreeVisitTask &task, std::deque<H5TreeVisitTask> &tasks) {
		H5TreeVisitContext visit_context;
		visit_context.scanner = this;
		visit_context.root_path = task.root_path;
		visit_context.root_ancestor_set = &task.ancestor_set;
		visit_context.group_stack.push_back({task.root_path, task.root_identity});
		visit_context.expanded_group_identities.insert(task.root_identity);

		std::lock_guard<std::recursive_mutex> lock(hdf5_global_mutex);
		H5ErrorSuppressor suppress;
		auto status =
		    task.root_path == "/"
		        ? H5Lvisit2(reader.GetFileHandle(), H5_INDEX_NAME, H5_ITER_NATIVE, VisitCallback, &visit_context)
		        : H5Lvisit_by_name2(reader.GetFileHandle(), task.root_path.c_str(), H5_INDEX_NAME, H5_ITER_NATIVE,
		                            VisitCallback, &visit_context, H5P_DEFAULT);
		if (status < 0 && !stop_requested) {
			throw IOException(FormatRemoteFileError(task.root_path == "/" ? "Failed to traverse HDF5 namespace"
			                                                              : "Failed to traverse HDF5 namespace subtree",
			                                        reader.GetFilename()));
		}
		for (auto &deferred : visit_context.deferred_tasks) {
			tasks.push_back(std::move(deferred));
		}
	}

	void BufferRow(H5TreeRow row) {
		if (stop_requested) {
			return;
		}
		buffered_rows.push_back(std::move(row));
		if (buffered_rows.size() >= STANDARD_VECTOR_SIZE) {
			FlushBufferedRows();
		}
	}

	void FlushBufferedRows() {
		if (buffered_rows.empty()) {
			return;
		}
		std::unique_lock<std::mutex> lock(state_lock);
		state_cv.wait(lock, [&] { return stop_requested || pending_rows.empty(); });
		if (stop_requested) {
			buffered_rows.clear();
			return;
		}
		pending_rows = std::move(buffered_rows);
		buffered_rows.clear();
		buffered_rows.reserve(STANDARD_VECTOR_SIZE);
		lock.unlock();
		state_cv.notify_one();
	}

	void ProduceRow(const std::string &path, const H5TreeResolvedEntry &resolved, hid_t parent_loc,
	                const char *link_name) {
		H5TreeRow row;
		row.path = path;
		row.type = H5TreeTypeName(resolved.type_kind);
		row.projected_values.resize(projected_attributes.size());
		if (resolved.identity) {
			reader.PopulateRowMetadataAndAttributes(row, resolved.type_kind, *resolved.identity, path, parent_loc,
			                                        link_name);
		}
		BufferRow(std::move(row));
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
			while (visit_context.group_stack.size() > 1 && visit_context.group_stack.back().path != parent_path) {
				visit_context.group_stack.pop_back();
			}
			if (visit_context.group_stack.empty() || visit_context.group_stack.back().path != parent_path) {
				throw IOException("Missing group stack entry for traversed group path: " + parent_path);
			}
			auto resolved = scanner.reader.ResolveEntry(full_path, *info, parent_loc, name);
			if (resolved.identity && H5TreeAncestorSetContains(*visit_context.root_ancestor_set, *resolved.identity)) {
				return scanner.stop_requested ? 1 : 0;
			}
			scanner.ProduceRow(full_path, resolved, parent_loc, name);
			if (resolved.traversable_group && resolved.identity) {
				if (resolved.is_soft_link) {
					H5TreeMaybeDeferGroupTraversal(visit_context, full_path, *resolved.identity);
				} else {
					auto inserted = visit_context.expanded_group_identities.insert(*resolved.identity).second;
					if (inserted) {
						visit_context.group_stack.push_back({full_path, *resolved.identity});
					} else {
						H5TreeMaybeDeferGroupTraversal(visit_context, full_path, *resolved.identity);
					}
				}
			}
			return scanner.stop_requested ? 1 : 0;
		} catch (const std::exception &ex) {
			scanner.MarkFailed(ex.what());
			return -1;
		}
	}
};

static unique_ptr<FunctionData> H5TreeBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<H5TreeBindData>();
	result->swmr = ResolveSwmrOption(context, input.named_parameters);
	auto expanded = H5ExpandFilePatterns(context, input.inputs[0], "h5_tree");
	result->filenames = std::move(expanded.filenames);
	result->had_glob = expanded.had_glob;

	names = {"path", "type", "dtype", "shape"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::LIST(LogicalType::UBIGINT)};
	H5TreeBindProjectedAttributes("h5_tree", input.inputs, 1, names, return_types, result->projected_attributes);
	auto filename_option = ResolveFilenameColumnOption(input.named_parameters);
	if (filename_option.include) {
		if (H5TreeOutputHasColumnName(names, filename_option.column_name)) {
			throw BinderException("Option filename adds column \"%s\", but that column name is already present in "
			                      "h5_tree output",
			                      filename_option.column_name);
		}
		result->visible_filename_idx = names.size();
		names.push_back(filename_option.column_name);
		return_types.push_back(LogicalType::VARCHAR);
	}
	return std::move(result);
}

static void H5TreePushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                        vector<unique_ptr<Expression>> &filters) {
	auto &bind_data = bind_data_p->Cast<H5TreeBindData>();
	H5ApplyFilenameFilterPushdown(context, get, bind_data.visible_filename_idx, bind_data.filenames, filters);
}

static void H5TreeOpenFileScanner(ClientContext &context, const H5TreeBindData &bind_data, H5TreeGlobalState &state,
                                  idx_t file_idx) {
	D_ASSERT(file_idx < bind_data.filenames.size());
	state.file_idx = file_idx;
	state.scanner = make_uniq<H5TreeScanner>(context, bind_data.filenames[file_idx], bind_data.swmr,
	                                         bind_data.projected_attributes);
}

static unique_ptr<GlobalTableFunctionState> H5TreeInit(ClientContext &context, TableFunctionInitInput &input) {
	ThrowIfInterrupted(context);
	auto &bind_data = input.bind_data->Cast<H5TreeBindData>();
	auto result = make_uniq<H5TreeGlobalState>();
	result->output_layout = H5TreeBuildOutputLayout(bind_data, input.column_ids);
	if (!bind_data.filenames.empty()) {
		H5TreeOpenFileScanner(context, bind_data, *result, 0);
	}
	return std::move(result);
}

static void H5TreeScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	ThrowIfInterrupted(context);
	auto &bind_data = data.bind_data->Cast<H5TreeBindData>();
	auto &gstate = data.global_state->Cast<H5TreeGlobalState>();
	vector<H5TreeRow> rows;
	if (!gstate.scanner) {
		D_ASSERT(bind_data.filenames.empty());
		output.SetCardinality(0);
		return;
	}
	while (true) {
		gstate.scanner->ReadRows(rows);
		if (!rows.empty()) {
			break;
		}
		auto next_file_idx = gstate.file_idx + 1;
		if (next_file_idx >= bind_data.filenames.size()) {
			output.SetCardinality(0);
			return;
		}
		H5TreeOpenFileScanner(context, bind_data, gstate, next_file_idx);
	}

	output.SetCardinality(rows.size());

	idx_t total_shape_elems = 0;
	idx_t shape_offset = 0;
	uint64_t *shape_data = nullptr;
	auto shape_output_idx = gstate.output_layout.shape_output_idx;
	if (shape_output_idx.has_value()) {
		for (auto &row : rows) {
			if (row.has_shape) {
				total_shape_elems += row.shape.size();
			}
		}
		auto &shape_vector = output.data[*shape_output_idx];
		ListVector::Reserve(shape_vector, total_shape_elems);
		auto &child = ListVector::GetEntry(shape_vector);
		shape_data = FlatVector::GetData<uint64_t>(child);
	}

	for (idx_t row_idx = 0; row_idx < rows.size(); row_idx++) {
		for (const auto &output_column : gstate.output_layout.projected_columns) {
			H5TreeWriteProjectedValue(rows[row_idx], bind_data.projected_attributes, output_column.column_id,
			                          output.data[output_column.output_idx], row_idx, shape_offset, shape_data);
		}
	}
	if (shape_output_idx.has_value()) {
		ListVector::SetListSize(output.data[*shape_output_idx], shape_offset);
	}
	H5TreePopulateFilenameColumns(bind_data.filenames[gstate.file_idx], gstate.output_layout.filename_output_idxs,
	                              output);
}

static void H5AttrFunction(DataChunk &args, ExpressionState &, Vector &result) {
	auto &children = StructVector::GetEntries(result);
	D_ASSERT(children.size() == 3);
	auto &tag_child = GetTreeStructChild(children[0]);
	auto &name_child = GetTreeStructChild(children[1]);
	auto &default_child = GetTreeStructChild(children[2]);

	if (args.ColumnCount() == 0) {
		for (idx_t i = 0; i < args.size(); i++) {
			FlatVector::GetData<string_t>(tag_child)[i] = StringVector::AddString(tag_child, "__attr_all__");
			FlatVector::SetNull(name_child, i, true);
			FlatVector::SetNull(default_child, i, true);
		}
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		result.Verify(args.size());
		return;
	}

	auto &name_vec = args.data[0];
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

	bool all_const = name_vec.GetVectorType() == VectorType::CONSTANT_VECTOR;
	if (args.ColumnCount() == 2) {
		auto &default_vec = args.data[1];
		default_child.Reference(default_vec);
		all_const = all_const && default_vec.GetVectorType() == VectorType::CONSTANT_VECTOR;
	} else {
		if (all_const) {
			default_child.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(default_child, true);
		} else {
			for (idx_t i = 0; i < args.size(); i++) {
				FlatVector::SetNull(default_child, i, true);
			}
		}
	}

	result.SetVectorType(all_const ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
	result.Verify(args.size());
}

static unique_ptr<FunctionData> H5AttrBind(ClientContext &, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() > 2) {
		throw InvalidInputException("h5_attr() accepts zero, one, or two arguments");
	}
	LogicalType default_type;
	LogicalType attribute_name_type = LogicalType::VARCHAR;
	if (arguments.size() == 2) {
		if (!arguments[1]->IsFoldable()) {
			throw InvalidInputException("h5_attr default_value must be a constant expression");
		}
		if (arguments[1]->return_type.id() == LogicalTypeId::SQLNULL) {
			throw InvalidInputException(
			    "h5_attr default_value must have a concrete type; use an explicit cast such as NULL::VARCHAR");
		}
		default_type = arguments[1]->return_type;
		attribute_name_type = arguments[0]->return_type;
	} else if (arguments.size() == 1) {
		default_type = LogicalType::VARIANT();
		attribute_name_type = arguments[0]->return_type;
	} else {
		default_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARIANT());
	}
	child_list_t<LogicalType> struct_children = {
	    {"tag", LogicalType::VARCHAR}, {"attribute_name", attribute_name_type}, {"default_value", default_type}};
	bound_function.return_type = LogicalType::STRUCT(struct_children);
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

void RegisterH5AttrFunction(ExtensionLoader &loader) {
	ScalarFunctionSet h5_attr("h5_attr");

	ScalarFunction h5_attr_zero("h5_attr", {}, LogicalTypeId::STRUCT, H5AttrFunction, H5AttrBind);
	h5_attr_zero.serialize = VariableReturnBindData::Serialize;
	h5_attr_zero.deserialize = VariableReturnBindData::Deserialize;
	h5_attr_zero.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	h5_attr.AddFunction(h5_attr_zero);

	ScalarFunction h5_attr_one("h5_attr", {LogicalType::VARCHAR}, LogicalTypeId::STRUCT, H5AttrFunction, H5AttrBind);
	h5_attr_one.serialize = VariableReturnBindData::Serialize;
	h5_attr_one.deserialize = VariableReturnBindData::Deserialize;
	h5_attr_one.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	h5_attr.AddFunction(h5_attr_one);

	ScalarFunction h5_attr_two("h5_attr", {LogicalType::VARCHAR, LogicalType::ANY}, LogicalTypeId::STRUCT,
	                           H5AttrFunction, H5AttrBind);
	h5_attr_two.serialize = VariableReturnBindData::Serialize;
	h5_attr_two.deserialize = VariableReturnBindData::Deserialize;
	h5_attr_two.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	h5_attr.AddFunction(h5_attr_two);

	loader.RegisterFunction(h5_attr);
}

void RegisterH5TreeFunction(ExtensionLoader &loader) {
	TableFunction h5_tree_function("h5_tree", {LogicalType::VARCHAR}, H5TreeScan, H5TreeBind, H5TreeInit);
	h5_tree_function.varargs = LogicalType::ANY;
	h5_tree_function.named_parameters["filename"] = LogicalType::ANY;
	h5_tree_function.named_parameters["swmr"] = LogicalType::BOOLEAN;
	// Projection pushdown is enabled so DuckDB can bind hidden virtual columns.
	// h5_tree still intentionally collects full object metadata for each emitted row.
	h5_tree_function.projection_pushdown = true;
	h5_tree_function.pushdown_complex_filter = H5TreePushdownComplexFilter;
	h5_tree_function.get_virtual_columns = H5GetFilenameVirtualColumns;
	loader.RegisterFunction(MultiFileReader::CreateFunctionSet(std::move(h5_tree_function)));
}

} // namespace duckdb
