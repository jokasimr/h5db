#include "h5_functions.hpp"
#include "h5_internal.hpp"
#include "h5_tree_shared.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
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
	std::string filename;
	vector<H5TreeProjectedAttributeSpec> projected_attributes;
	bool swmr = false;
};

struct H5TreeGlobalState : public GlobalTableFunctionState {
	unique_ptr<class H5TreeScanner> scanner;

	idx_t MaxThreads() const override {
		return 1;
	}
};

struct H5TreeGroupStackEntry {
	std::string path;
	H5TreeObjectIdentity identity;
};

struct H5TreeRowBatch {
	std::vector<H5TreeRow> rows;
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
	H5TreeScanner(ClientContext &context_p, const H5TreeBindData &bind_data_p)
	    : context(context_p), bind_data(bind_data_p),
	      reader(context_p, bind_data_p.filename, bind_data_p.swmr, bind_data_p.projected_attributes) {
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
			return;
		}
		auto batch = std::move(row_queue.front());
		row_queue.pop_front();
		lock.unlock();
		queue_not_full.notify_one();
		rows = std::move(batch.rows);
	}

private:
	static constexpr idx_t MAX_ROW_QUEUE_BATCHES = 8;

	ClientContext &context;
	const H5TreeBindData &bind_data;
	H5TreeFileReader reader;

	std::mutex queue_lock;
	std::condition_variable queue_not_empty;
	std::condition_variable queue_not_full;
	std::deque<H5TreeRowBatch> row_queue;
	std::vector<H5TreeRow> buffered_rows;
	std::thread worker_thread;
	std::atomic<bool> stop_requested {false};
	bool worker_started = false;
	bool worker_done = false;
	bool worker_failed = false;
	std::string worker_error;

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
		queue_not_full.notify_all();
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
		queue_not_full.notify_all();
	}

	void MarkFailed(const std::string &message) {
		std::lock_guard<std::mutex> lock(queue_lock);
		stop_requested = true;
		worker_done = true;
		worker_failed = true;
		worker_error = H5TreeNormalizeExceptionMessage(message);
		queue_not_empty.notify_all();
		queue_not_full.notify_all();
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
		row.projected_values.resize(bind_data.projected_attributes.size());
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
			throw IOException(task.root_path == "/" ? "Failed to traverse HDF5 namespace"
			                                        : "Failed to traverse HDF5 namespace subtree");
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
		std::unique_lock<std::mutex> lock(queue_lock);
		queue_not_full.wait(lock, [&] { return stop_requested || row_queue.size() < MAX_ROW_QUEUE_BATCHES; });
		if (stop_requested) {
			buffered_rows.clear();
			return;
		}
		H5TreeRowBatch batch;
		batch.rows = std::move(buffered_rows);
		row_queue.push_back(std::move(batch));
		buffered_rows.clear();
		buffered_rows.reserve(STANDARD_VECTOR_SIZE);
		queue_not_empty.notify_one();
	}

	void ProduceRow(const std::string &path, const H5TreeResolvedEntry &resolved, hid_t parent_loc,
	                const char *link_name) {
		H5TreeRow row;
		row.path = path;
		row.type = H5TreeTypeName(resolved.type_kind);
		row.projected_values.resize(bind_data.projected_attributes.size());
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
	if (input.inputs.empty()) {
		throw InvalidInputException("h5_tree requires at least 1 argument: filename");
	}
	result->filename = GetRequiredStringArgument(input.inputs[0], "h5_tree", "filename");
	result->swmr = ResolveSwmrOption(context, input.named_parameters);

	names = {"path", "type", "dtype", "shape"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::LIST(LogicalType::UBIGINT)};
	H5TreeBindProjectedAttributes("h5_tree", input.inputs, 1, names, return_types, result->projected_attributes);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> H5TreeInit(ClientContext &context, TableFunctionInitInput &input) {
	ThrowIfInterrupted(context);
	auto &bind_data = input.bind_data->Cast<H5TreeBindData>();
	auto result = make_uniq<H5TreeGlobalState>();
	result->scanner = make_uniq<H5TreeScanner>(context, bind_data);
	return std::move(result);
}

static void H5TreeScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	ThrowIfInterrupted(context);
	auto &bind_data = data.bind_data->Cast<H5TreeBindData>();
	auto &gstate = data.global_state->Cast<H5TreeGlobalState>();
	auto expected_columns = 4 + bind_data.projected_attributes.size();
	if (output.ColumnCount() != expected_columns) {
		throw InternalException("h5_tree expected %llu output columns, got %llu", expected_columns,
		                        output.ColumnCount());
	}
	vector<H5TreeRow> rows;
	gstate.scanner->ReadRows(rows);
	if (rows.empty()) {
		output.SetCardinality(0);
		return;
	}

	output.SetCardinality(rows.size());

	idx_t total_shape_elems = 0;
	uint64_t *shape_data = nullptr;
	idx_t shape_offset = 0;
	for (auto &row : rows) {
		if (row.has_shape) {
			total_shape_elems += row.shape.size();
		}
	}
	auto &shape_vector = output.data[3];
	ListVector::Reserve(shape_vector, total_shape_elems);
	auto &child = ListVector::GetEntry(shape_vector);
	shape_data = FlatVector::GetData<uint64_t>(child);

	for (idx_t row_idx = 0; row_idx < rows.size(); row_idx++) {
		H5TreeWriteRow(rows[row_idx], bind_data.projected_attributes, output, row_idx, shape_offset, shape_data);
	}
	ListVector::SetListSize(shape_vector, shape_offset);
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
	TableFunction h5_tree("h5_tree", {LogicalType::VARCHAR}, H5TreeScan, H5TreeBind, H5TreeInit);
	h5_tree.varargs = LogicalType::ANY;
	h5_tree.named_parameters["swmr"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(h5_tree);
}

} // namespace duckdb
