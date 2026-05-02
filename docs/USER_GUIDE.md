# h5db User Guide

This guide is a practical introduction to using `h5db`. It focuses on the part
of the extension that most users need first:

- inspect a file
- find the paths you care about
- read datasets as columns
- read attributes
- handle a few common HDF5 patterns

It is intentionally not a full API reference. For complete details, see
[API.md](API.md).

## Minimal Cheat Sheet

```sql
-- Inspect the full structure
FROM h5_tree('data.h5');

-- Inspect one group
FROM h5_ls('data.h5', '/entry');

-- Read datasets as columns
FROM h5_read('data.h5', '/time', '/counts');

-- Read one object's attributes
FROM h5_attributes('data.h5', '/entry/data');

-- Browse structure with one projected attribute
SELECT path, NX_class
FROM h5_tree('data.h5', h5_attr('NX_class'));
```

## What h5db Is For

`h5db` lets DuckDB query HDF5 files directly with SQL.

That means you can use HDF5 files in the same workflow as other DuckDB data
sources:

- inspect structure with SQL
- read selected datasets into a table
- filter, join, aggregate, and export results
- work with local files or remote URLs

The key idea is:

- HDF5 is a hierarchical namespace of groups, datasets, links, and attributes
- `h5db` gives you SQL functions that expose those pieces in useful ways

The four functions most users should learn first are:

- `h5_tree(...)`: recursively inspect the file structure
- `h5_ls(...)`: list one level of a group
- `h5_read(...)`: read datasets as table columns
- `h5_attributes(...)`: read all attributes from the same object path in one or more files

If you understand those four, you can already do most day-to-day work with
`h5db`.

## Load the Extension

In DuckDB:

```sql
INSTALL h5db FROM community;
LOAD h5db;
```

## Step 1: Inspect the File

When you open an unfamiliar HDF5 file, start by looking at its namespace.

### Recursive view: `h5_tree(...)`

```sql
FROM h5_tree('data.h5');
```

This returns one row per path with:

- `path`
- `type`
- `dtype`
- `shape`

Use this when you want the big picture.

Typical workflow:

```sql
FROM h5_tree('data.h5')
ORDER BY path;
```

Here the DuckDB `.maxrows N` command can be useful when you want to see the entire output.

You can apply regular SQL filters to limit the number of rows displayed:

For example:

```sql
FROM h5_tree('data.h5')
WHERE path LIKE '/entry/instrument/detector/%'
ORDER BY path;
```

This only displays entries below `/entry/instrument/detector/`.

What to look for:

- groups that organize the file
- dataset paths you want to read
- whether a dataset is scalar or array-shaped

### Shallow view: `h5_ls(...)`

If `h5_tree(...)` is too much, inspect one level at a time:

```sql
FROM h5_ls('data.h5');

FROM h5_ls('data.h5', '/entry/instrument');
```

Use `h5_ls(...)` when you already know roughly where to look and want a more
focused view.

There is also a scalar form for queries that need one value per input row:

```sql
SELECT h5_ls('data.h5', '/entry/instrument');
```

The scalar form returns a `MAP(VARCHAR, STRUCT(...))` keyed by child name.

## Step 2: Read Datasets as Columns

Once you know the dataset paths, use `h5_read(...)`.

### Read one dataset

```sql
FROM h5_read('data.h5', '/measurements');
```

This is the most direct way to turn HDF5 data into a DuckDB table.

### Read multiple datasets side by side

```sql
FROM h5_read(
    'data.h5',
    '/timestamps',
    '/temperatures'
);
```

Think of this as "read these datasets and align them by row".

This works best when the datasets share the same outer dimension.

### Add a virtual row index

Many HDF5 datasets do not store an explicit row-number column. If you want one:

```sql
FROM h5_read(
    'data.h5',
    h5_index(),
    '/measurements'
);
```

This is mainly useful for

- filtering out a subset of data
- joining other datasets by position

If `h5_read(...)` reads multiple files, `h5_index()` still means the row index
within the current file's dataset, so it starts at `0` for each file.

### Read multiple files

The table-valued `h5_tree(...)`, `h5_ls(...)`, `h5_read(...)`, and
`h5_attributes(...)` can read more than one file at a time.

You can use glob patterns with local paths and `sftp://` URLs, and with
DuckDB-backed remote schemes when the underlying DuckDB filesystem supports
globbing:

```sql
FROM h5_read('runs/run_*.h5', '/counts');

FROM h5_tree('runs/**/*.h5');
```

You can also pass an explicit `VARCHAR[]` when you want a defined expansion
order or want to mix exact files with patterns:

```sql
FROM h5_read(
    ['runs/calibration.h5', 'runs/run_*.h5'],
    '/counts'
);

SELECT filename, units
FROM h5_attributes('runs/run_*.h5', '/entry/data');
```

Useful mental models:

- `h5_read(...)` concatenates rows file by file
- `h5_attributes(...)` returns one row per matched file
- `h5_index()` is per file, not across the combined multi-file result
- list entries are expanded left-to-right
- duplicate files are preserved
- table `h5_tree(...)`, table `h5_ls(...)`, `h5_read(...)`, and `h5_attributes(...)` expose a hidden virtual
  `filename` column
- `filename := true` adds `filename` to the visible output schema, which is useful when you want it included in
  `SELECT *`
- a pattern that matches no files raises an error
- `h5_read(...)` requires compatible column definitions across all matched files
- `h5_attributes(...)` requires the same attribute names, types, and order across all matched files
- scalar `h5_ls(...)` accepts one filename expression per row and does not expand filename lists or glob patterns
- for local paths and DuckDB-backed remote schemes, glob expansion uses
  DuckDB's filesystem stack
- for `sftp://` URLs, glob expansion is handled by h5db's SFTP backend
- in both cases, glob expansion follows the same semantics as DuckDB's other
  multi-file readers such as `read_parquet(...)`
- in particular, recursive `**` does not traverse symlink directories

`h5_tree(...)` and table-valued `h5_ls(...)` can use selective filters on the virtual `filename` column to skip
expanded files before opening them:

```sql
SELECT path, type
FROM h5_tree('runs/**/*.h5')
WHERE filename LIKE '%/run_042.h5';

SELECT filename, path
FROM h5_ls('runs/run_*.h5', '/entry')
WHERE filename IN ('runs/run_001.h5', 'runs/run_002.h5');
```

This works for filename-only filters, and for filename filters combined with other predicates using `AND`, when DuckDB
pushes the filter into the table function. Filters whose values come from joins, semi-joins, scalar subqueries, or later
query blocks may still open all expanded files. `h5_read(...)` and `h5_attributes(...)` do not currently use
`filename` filters to avoid opening files.

When you need to keep track of which file produced a row, you can select the hidden `filename` column explicitly:

```sql
SELECT filename, path
FROM h5_tree('runs/**/*.h5')
WHERE path = '/entry/data';
```

Or use `filename := true` when you want `filename` included in `SELECT *`:

```sql
FROM h5_tree('runs/**/*.h5', filename := true)
WHERE path = '/entry/data';
```

### Rename output columns

HDF5 datasets with the same final path component produce the same DuckDB column name. DuckDB compares output column
names case-insensitively, so names such as `temperature` and `Temperature` also collide. Use `h5_alias(...)` to give
one or both columns explicit names:

```sql
FROM h5_read(
    'data.h5',
    h5_alias('sun_temp', '/sun/temperatures'),
    h5_alias('moon_temp', '/moon/temperatures')
);
```

## Step 3: Read Attributes

HDF5 attributes often hold the metadata that makes a dataset meaningful:

- units
- descriptions
- NeXus classes
- instrument metadata
- coordinate metadata

### Read all attributes from one object

```sql
FROM h5_attributes('data.h5', '/measurements');
```
This returns one row where each attribute becomes its own column.

Use this when you are inspecting one specific object path and want full detail.
With multiple files, the object must expose the same attribute names, types, and
order in every file.

### Read root attributes

File-level metadata often lives on `/`:

```sql
FROM h5_attributes('data.h5', '/');
```

## Step 4: Combine Structure and Attributes

A common pattern is:

1. use `h5_tree(...)` to find interesting paths
2. project a few attributes while browsing
3. then use `h5_read(...)` or `h5_attributes(...)` on the exact objects you care
   about

### Project selected attributes in `h5_tree(...)`

```sql
FROM h5_tree(
    'data.h5',
    h5_attr('NX_class')
)
ORDER BY path;
```

This is especially useful in NeXus files, where `NX_class` tells you what a
group means.

### Project all attributes as one map

```sql
SELECT path, h5_attr
FROM h5_tree(
    'data.h5',
    h5_attr()
)
ORDER BY path;
```

This is useful for broad exploration, but it can be a lot of information. Start
with a few selected attributes when possible.

## A Good Exploration Pattern

If you are opening an unfamiliar file, this sequence works well:

### 1. Look at the whole structure

```sql
FROM h5_tree('data.h5')
ORDER BY path;
```

### 2. Add one or two important attributes

```sql
FROM h5_tree(
    'data.h5',
    h5_attr('NX_class')
)
ORDER BY path;
```

### 3. Read one object's attributes in detail

```sql
FROM h5_attributes('data.h5', '/entry/data');
```

### 4. Read the datasets you actually need

```sql
FROM h5_read(
    'data.h5',
    '/entry/data/time',
    '/entry/data/counts'
);
```

## Remote Files

All major h5db functions accept local paths or remote URLs as `filename`.

Examples:

```sql
FROM h5_tree('https://example.com/data.h5');

FROM h5_read('s3://bucket/data.h5', '/measurements');
```

For `sftp://` URLs, you first create a DuckDB secret of type `sftp`.

Available authentication methods are:

- `USE_AGENT true`
- `PASSWORD '...'`
- `KEY_PATH '...'` with optional `KEY_PASSPHRASE '...'`

Recommended default: if you already use an SSH agent / OS keychain integration, prefer `USE_AGENT true`. It avoids
storing the SSH password or private-key passphrase in the DuckDB secret. Password and explicit key-file auth are also
supported.

Example:

```sql
CREATE OR REPLACE SECRET beamline_sftp (
    TYPE sftp,
    SCOPE 'sftp://beamline.example.org/',
    USERNAME 'alice',
    USE_AGENT true,
    KNOWN_HOSTS_PATH '/home/alice/.ssh/known_hosts'
);
```

Then query the file normally:

```sql
FROM h5_read(
    'sftp://beamline.example.org/data/run001.h5',
    '/entry/data'
);

FROM h5_read(
    'sftp://beamline.example.org/data/run_*.h5',
    '/entry/data'
);
```

See [API.md](API.md) for the full SFTP secret specification.

## A Few Important Mental Models

### 1. Paths matter

`h5db` works with HDF5 paths, so getting comfortable with the file's namespace is
the main skill.

If a query is not doing what you expect, check the exact paths again with
`h5_tree(...)` or `h5_ls(...)`.

### 2. `h5_tree(...)` is for discovery, `h5_read(...)` is for data

Do not try to use `h5_tree(...)` for everything.

Use:

- `h5_tree(...)` to discover
- `h5_attributes(...)` to inspect metadata on one object
- `h5_read(...)` to do the actual analysis

### 3. Attributes are metadata, not columns

Datasets become table columns in `h5_read(...)`.

Attributes are usually better treated as:

- one-off metadata from `h5_attributes(...)`
- or light annotation during browsing with `h5_attr(...)`

### 4. HDF5 files are heterogeneous

Different producers use different conventions:

- plain HDF5
- NeXus

That is normal. Start by inspecting the file before assuming a particular shape.

## Common Mistakes

### Reading before inspecting

If you guess dataset paths, you will waste time. Start with:

```sql
FROM h5_tree('data.h5');
```

### Using `h5_attributes(...)` when you only want one projected field

If you are already browsing with `h5_tree(...)`, often this is enough:

```sql
SELECT path, NX_class
FROM h5_tree('data.h5', h5_attr('NX_class'));
```

Use `h5_attributes(...)` when you want the full attribute set of a single object.

### Expecting every HDF5 type to work

`h5db` supports a large useful subset of HDF5, but not every type or encoding.

If you hit an unsupported type, check [API.md](API.md) for the current
limitations before assuming the file is corrupt.

## Where to Go Next

Once you are comfortable with the basics, the next most useful topics are:

- projected attributes with `h5_attr(...)` in `h5_tree(...)` and `h5_ls(...)`
- run-start encoded columns with `h5_rse(...)`
- remote access over HTTP/S3/SFTP
- exact type and error behavior

For those, continue with:

- [API.md](API.md)
- [RSE_USAGE.md](RSE_USAGE.md)
