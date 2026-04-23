# h5db Usage Guide

This guide is a practical introduction to using `h5db`. It focuses on the part
of the extension that most users are most likely to need:

- inspect a file
- find the paths you care about
- read datasets as columns
- read attributes
- handle a few common HDF5 patterns

It is intentionally not a full API reference. For complete details, see
[API.md](API.md).

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
- `h5_attributes(...)`: read attributes from one object

If you understand those four, you can already do most day-to-day work with
`h5db`.

## Load the Extension

In DuckDB:

```sql
INSTALL h5db FROM community;
LOAD h5db;
```

If you are using a local build of DuckDB with `h5db` built in, only `LOAD h5db`
may be needed.

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
SELECT path, type, dtype, shape
FROM h5_tree('data.h5')
ORDER BY path;
```

What to look for:

- groups that organize the file
- dataset paths you want to read
- whether a dataset is scalar or array-shaped
- whether names follow a convention such as NeXus

### Shallow view: `h5_ls(...)`

If `h5_tree(...)` is too much, inspect one level at a time:

```sql
FROM h5_ls('data.h5');

FROM h5_ls('data.h5', '/entry/instrument');
```

Use `h5_ls(...)` when you already know roughly where to look and want a more
focused view.

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

Think of this as “read these datasets and align them by row”.

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

This is often useful for:

- debugging
- joining against derived tables
- checking row alignment between datasets

### Rename output columns

Some HDF5 paths are long or repetitive. Use `h5_alias(...)` to make query output
easier to work with:

```sql
FROM h5_read(
    'data.h5',
    h5_alias('time', '/timestamps'),
    h5_alias('temp', '/temperatures')
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

Use this when you are inspecting one specific object and want full detail.

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
SELECT path, type, NX_class
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
SELECT path, type, dtype, shape
FROM h5_tree('data.h5')
ORDER BY path;
```

### 2. Add one or two important attributes

```sql
SELECT path, type, dtype, shape, NX_class
FROM h5_tree(
    'data.h5',
    h5_attr('NX_class')
)
ORDER BY path;
```

### 3. Read one object’s attributes in detail

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

That pattern is enough for a large fraction of real use cases.

## Remote Files

All major h5db functions accept local paths or remote URLs as `filename`.

Examples:

```sql
FROM h5_tree('https://example.com/data.h5');

FROM h5_read('s3://bucket/data.h5', '/measurements');
```

For `sftp://` URLs, you first create a DuckDB secret of type `sftp`.

Example:

```sql
CREATE OR REPLACE SECRET beamline_sftp (
    TYPE sftp,
    SCOPE 'sftp://beamline.example.org/',
    USERNAME 'alice',
    PASSWORD 'secret',
    KNOWN_HOSTS_PATH '/home/alice/.ssh/known_hosts'
);
```

Then query the file normally:

```sql
FROM h5_read(
    'sftp://beamline.example.org/data/run001.h5',
    '/entry/data'
);
```

See [API.md](API.md) for the full SFTP secret specification.

## A Few Important Mental Models

### 1. Paths matter

`h5db` works with HDF5 paths, so getting comfortable with the file’s namespace is
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
- library-specific layouts such as Scipp
- custom instrument-specific schemas

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
