# Run-Start Encoding Implementation Plan

## API Design

### Core Concept

Extend `h5_read()` to accept run-start encoded (RSE) column specifications via helper function.

**Syntax**:
```sql
SELECT * FROM h5_read(
    'file.h5',
    'regular_column_A',
    h5_rse('run_starts_dataset', 'values_dataset'),
    'regular_column_B'
);
```

**Example**:
```sql
-- File structure:
--   /timestamp[10] - regular dataset
--   /state_run_starts[3] - run starts: [0, 3, 7]
--   /state_values[3] - values: [100, 200, 300]

SELECT * FROM h5_read('file.h5', 'timestamp', h5_rse('state_run_starts', 'state_values'));

-- Returns:
┌───────────┬───────┐
│ timestamp │ state │  ← Column named 'state' (from values dataset)
├───────────┼───────┤
│         0 │   100 │
│         1 │   100 │
│         2 │   100 │
│         3 │   200 │
│         4 │   200 │
│         5 │   200 │
│         6 │   200 │
│         7 │   300 │
│         8 │   300 │
│         9 │   300 │
└───────────┴───────┘
```

### Helper Function: `h5_rse(run_starts, values)`

**Type**: Scalar function returning STRUCT
**Parameters**:
- `run_starts` (VARCHAR): Path to run starts dataset
- `values` (VARCHAR): Path to values dataset

**Returns**: `STRUCT(encoding VARCHAR, run_starts VARCHAR, values VARCHAR)`

**Example**:
```sql
SELECT h5_rse('state_run_starts', 'state_values');
-- Returns: {'encoding': 'rse', 'run_starts': 'state_run_starts', 'values': 'state_values'}
```

### Benefits

1. **Explicit**: No naming convention dependencies
2. **Transparent**: Clear what's happening
3. **Extensible**: Easy to add `h5_rle()`, `h5_ree()`, etc.
4. **Reuses existing function**: No new table function needed
5. **Flexible**: Mix regular and encoded columns freely

## Implementation

### 1. Add `h5_rse()` Scalar Function

```cpp
static void H5RseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &run_starts = args.data[0];
    auto &values = args.data[1];

    auto run_starts_data = FlatVector::GetData<string_t>(run_starts);
    auto values_data = FlatVector::GetData<string_t>(values);

    // Create struct: {encoding: 'rse', run_starts: ..., values: ...}
    auto result_data = FlatVector::GetData<struct_entry_t>(result);
    auto &children = StructVector::GetEntries(result);

    for (idx_t i = 0; i < args.size(); i++) {
        // Set encoding field
        children[0]->SetValue(i, Value("rse"));
        // Set run_starts field
        children[0]->SetValue(i, run_starts_data[i].GetString());
        // Set values field
        children[1]->SetValue(i, values_data[i].GetString());
    }
}

void RegisterH5RseFunction(ExtensionLoader &loader) {
    auto h5_rse = ScalarFunction(
        "h5_rse",
        {LogicalType::VARCHAR, LogicalType::VARCHAR},
        LogicalType::STRUCT({
            {"encoding", LogicalType::VARCHAR},
            {"run_starts", LogicalType::VARCHAR},
            {"values", LogicalType::VARCHAR}
        }),
        H5RseFunction
    );
    loader.RegisterFunction(h5_rse);
}
```

### 2. Update `h5_read()` to Handle Encoded Columns

#### Data Structures

```cpp
enum class ColumnType {
    REGULAR,
    RUN_START_ENCODED
};

struct ColumnSpec {
    std::string name;
    ColumnType type;
    LogicalType duckdb_type;

    // For regular columns
    std::string dataset_path;
    DatasetInfo dataset_info;  // Existing structure

    // For RSE columns
    std::string run_starts_path;
    std::string values_path;
    hid_t run_starts_type_id;
    hid_t values_type_id;
};

struct H5ReadBindData : public TableFunctionData {
    std::string filename;
    std::vector<ColumnSpec> columns;
    hsize_t num_rows;  // From shortest regular column
};

struct H5ReadGlobalState : public GlobalTableFunctionState {
    hid_t file_id;
    std::vector<hid_t> dataset_ids;  // Regular datasets

    // RSE columns: read once at init
    struct RSEColumn {
        std::vector<idx_t> run_starts;
        std::vector<Value> values;
        idx_t current_run;
        idx_t next_run_start;
    };
    std::vector<RSEColumn> rse_columns;

    idx_t position;
};
```

#### Bind Phase

```cpp
static unique_ptr<FunctionData> H5ReadBind(...) {
    auto result = make_uniq<H5ReadBindData>();
    result->filename = input.inputs[0].GetValue<string>();

    // Open file temporarily
    hid_t file_id = H5Fopen(result->filename.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);

    hsize_t min_rows = std::numeric_limits<hsize_t>::max();

    // Process each argument after filename
    for (size_t i = 1; i < input.inputs.size(); i++) {
        auto &input_val = input.inputs[i];

        if (input_val.type().id() == LogicalTypeId::STRUCT) {
            // RSE column
            auto &struct_val = StructValue::GetChildren(input_val);
            std::string encoding = struct_val[0].GetValue<string>();
            std::string run_starts = struct_val[1].GetValue<string>();
            std::string values = struct_val[2].GetValue<string>();

            if (encoding != "rse") {
                throw InvalidInputException("Unknown encoding: " + encoding);
            }

            ColumnSpec col;
            col.type = ColumnType::RUN_START_ENCODED;
            col.run_starts_path = run_starts;
            col.values_path = values;

            // Open values dataset to get type and name
            hid_t values_ds = H5Dopen2(file_id, values.c_str(), H5P_DEFAULT);
            hid_t type_id = H5Dget_type(values_ds);
            col.duckdb_type = H5TypeToDuckDBType(type_id);
            col.values_type_id = H5Tcopy(type_id);

            // Column name from values dataset
            col.name = GetColumnName(values);

            H5Tclose(type_id);
            H5Dclose(values_ds);

            // Open run_starts to get type
            hid_t starts_ds = H5Dopen2(file_id, run_starts.c_str(), H5P_DEFAULT);
            hid_t starts_type = H5Dget_type(starts_ds);
            col.run_starts_type_id = H5Tcopy(starts_type);
            H5Tclose(starts_type);
            H5Dclose(starts_ds);

            result->columns.push_back(col);

        } else {
            // Regular column
            std::string dataset_path = input_val.GetValue<string>();

            ColumnSpec col;
            col.type = ColumnType::REGULAR;
            col.dataset_path = dataset_path;
            col.name = GetColumnName(dataset_path);

            // Get type and dimensions (existing code)
            hid_t dataset_id = H5Dopen2(file_id, dataset_path.c_str(), H5P_DEFAULT);
            // ... existing dataset inspection logic ...

            // Track minimum rows from regular columns only
            if (col.dataset_info.dims[0] < min_rows) {
                min_rows = col.dataset_info.dims[0];
            }

            result->columns.push_back(col);
            H5Dclose(dataset_id);
        }
    }

    H5Fclose(file_id);

    result->num_rows = min_rows;

    // Build return schema
    for (const auto &col : result->columns) {
        names.push_back(col.name);
        return_types.push_back(col.duckdb_type);
    }

    return std::move(result);
}
```

#### Init Phase

```cpp
static unique_ptr<GlobalTableFunctionState> H5ReadInit(...) {
    auto result = make_uniq<H5ReadGlobalState>();
    auto &bind_data = data.Cast<H5ReadBindData>();

    result->file_id = H5Fopen(bind_data.filename.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);

    for (const auto &col : bind_data.columns) {
        if (col.type == ColumnType::REGULAR) {
            // Open regular dataset (existing code)
            hid_t dataset_id = H5Dopen2(result->file_id, col.dataset_path.c_str(), H5P_DEFAULT);
            result->dataset_ids.push_back(dataset_id);

        } else if (col.type == ColumnType::RUN_START_ENCODED) {
            // Read entire run_starts and values arrays
            RSEColumn rse;

            // Read run_starts
            hid_t starts_ds = H5Dopen2(result->file_id, col.run_starts_path.c_str(), H5P_DEFAULT);
            hid_t starts_space = H5Dget_space(starts_ds);
            hsize_t num_runs;
            H5Sget_simple_extent_dims(starts_space, &num_runs, nullptr);

            std::vector<int64_t> starts_data(num_runs);
            H5Dread(starts_ds, H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT, starts_data.data());

            rse.run_starts.resize(num_runs);
            for (size_t i = 0; i < num_runs; i++) {
                rse.run_starts[i] = starts_data[i];
            }

            H5Sclose(starts_space);
            H5Dclose(starts_ds);

            // Read values
            hid_t values_ds = H5Dopen2(result->file_id, col.values_path.c_str(), H5P_DEFAULT);
            hid_t values_space = H5Dget_space(values_ds);
            hsize_t num_values;
            H5Sget_simple_extent_dims(values_space, &num_values, nullptr);

            // Read based on type
            if (col.values_type_id == H5T_NATIVE_INT32) {
                std::vector<int32_t> vals(num_values);
                H5Dread(values_ds, H5T_NATIVE_INT32, H5S_ALL, H5S_ALL, H5P_DEFAULT, vals.data());
                for (auto v : vals) rse.values.push_back(Value::INTEGER(v));
            }
            // ... handle other types ...

            H5Sclose(values_space);
            H5Dclose(values_ds);

            // Initialize scan state
            rse.current_run = 0;
            rse.next_run_start = (rse.run_starts.size() > 1)
                                 ? rse.run_starts[1]
                                 : bind_data.num_rows;

            result->rse_columns.push_back(rse);
        }
    }

    result->position = 0;
    return std::move(result);
}
```

#### Scan Phase - Single Pass Algorithm

```cpp
static void H5ReadScan(...) {
    auto &bind_data = data.bind_data->Cast<H5ReadBindData>();
    auto &gstate = data.global_state->Cast<H5ReadGlobalState>();

    idx_t remaining = bind_data.num_rows - gstate.position;
    if (remaining == 0) {
        output.SetCardinality(0);
        return;
    }

    idx_t to_read = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);
    idx_t regular_col_idx = 0;
    idx_t rse_col_idx = 0;

    for (size_t col_idx = 0; col_idx < bind_data.columns.size(); col_idx++) {
        const auto &col = bind_data.columns[col_idx];
        auto &result_vector = output.data[col_idx];

        if (col.type == ColumnType::REGULAR) {
            // Read regular column (existing code)
            hid_t dataset_id = gstate.dataset_ids[regular_col_idx++];
            // ... existing hyperslab read logic ...

        } else if (col.type == ColumnType::RUN_START_ENCODED) {
            // Expand RSE column - SINGLE PASS
            auto &rse = gstate.rse_columns[rse_col_idx++];

            // Reset to current position's run if needed
            idx_t current_row = gstate.position;

            // Find which run current_row is in
            while (current_row >= rse.next_run_start) {
                rse.current_run++;
                rse.next_run_start = (rse.current_run + 1 < rse.run_starts.size())
                                     ? rse.run_starts[rse.current_run + 1]
                                     : bind_data.num_rows;
            }

            // Expand values for this chunk
            auto *output_data = FlatVector::GetData<int32_t>(result_vector);  // Example for int32

            for (idx_t i = 0; i < to_read; i++) {
                idx_t row = gstate.position + i;

                // Advance run if needed
                while (row >= rse.next_run_start) {
                    rse.current_run++;
                    rse.next_run_start = (rse.current_run + 1 < rse.run_starts.size())
                                         ? rse.run_starts[rse.current_run + 1]
                                         : bind_data.num_rows;
                }

                // Emit current run's value
                output_data[i] = rse.values[rse.current_run].GetValue<int32_t>();
            }
        }
    }

    gstate.position += to_read;
    output.SetCardinality(to_read);
}
```

**Algorithm complexity**: O(1) amortized per row
- `current_run` only advances forward, never resets
- Total advances across all scan calls = num_runs
- Amortized over num_rows calls = O(num_runs / num_rows) ≈ O(1/compression_ratio)

## Testing

### Test Cases

```sql
-- 1. Simple RSE column
SELECT * FROM h5_read('test/data/run_encoded.h5',
    'experiment1/timestamp',
    h5_rse('experiment1/state_run_starts', 'experiment1/state_values')
) LIMIT 5;

-- Expected: 5 rows, columns: timestamp, state

-- 2. Multiple RSE columns
SELECT * FROM h5_read('test/data/run_encoded.h5',
    'experiment2/measurement',
    h5_rse('experiment2/status_run_starts', 'experiment2/status_values'),
    'experiment2/sensor_id'
);

-- Expected: 1000 rows, columns: measurement, status, sensor_id

-- 3. Only RSE column
SELECT * FROM h5_read('test/data/run_encoded.h5',
    h5_rse('experiment1/state_run_starts', 'experiment1/state_values')
);

-- Expected: ERROR - no regular column to determine row count

-- 4. String values
SELECT * FROM h5_read('test/data/run_encoded.h5',
    'experiment3/time',
    h5_rse('experiment3/level_run_starts', 'experiment3/level_values')
);

-- Expected: 8 rows, level column with string values
```

## Future Extensions

### Run-Length Encoding

```sql
-- h5_rle(run_lengths, values)
SELECT * FROM h5_read('file.h5',
    'timestamp',
    h5_rle('state_run_lengths', 'state_values')  -- [3, 4, 3] instead of [0, 3, 7]
);
```

### Run-End Encoding

```sql
-- h5_ree(run_ends, values)
SELECT * FROM h5_read('file.h5',
    'timestamp',
    h5_ree('state_run_ends', 'state_values')  -- [2, 6, 9] instead of [0, 3, 7]
);
```

### Custom Encoding

```sql
-- Generic: h5_encoded(encoding_type, metadata...)
SELECT * FROM h5_read('file.h5',
    'timestamp',
    h5_encoded('delta', 'initial_value', 'deltas')
);
```

## Summary

**Advantages of this design**:
1. ✅ Explicit and transparent
2. ✅ No naming conventions required
3. ✅ Extensible to other encodings
4. ✅ Reuses existing `h5_read()` infrastructure
5. ✅ Single-pass O(1) expansion algorithm
6. ✅ Column name from values dataset (intuitive)

**Implementation effort**: ~2-3 days
- Add `h5_rse()` scalar function: 2 hours
- Update `h5_read()` bind/init/scan: 1 day
- Testing and edge cases: 1 day
- Documentation: 2 hours
