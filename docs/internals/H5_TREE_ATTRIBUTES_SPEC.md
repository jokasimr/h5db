# h5_tree Attribute Projection Spec

Status: implemented and current

## Summary

`h5_tree(...)` supports projecting selected HDF5 attributes as additional output
columns. The same projected-attribute syntax is also reused by `h5_ls(...)`.
Projected attributes are declared with `h5_attr()`, `h5_attr(name)`, or
`h5_attr(name, default_value)` and may be renamed with `h5_alias(...)`.

Example:

```sql
SELECT path, type, NX_class, time
FROM h5_tree(
  'mccode.h5',
  h5_attr('NX_class', NULL::VARCHAR),
  h5_alias('time', h5_attr('count_time', 0::DOUBLE))
);
```

## API

`h5_tree` accepts:

```sql
h5_tree(filename, projected_attribute_or_alias..., swmr := <bool>)
```

Projected arguments must be one of:

- `h5_attr()`
- `h5_attr(name)`
- `h5_attr(name, default_value)`
- `h5_alias(alias_name, h5_attr(...))`

`h5_alias(...)` follows the same alias semantics here that it already uses in
`h5_read(...)`.

## Output Schema

The base `h5_tree` columns remain:

- `path`
- `type`
- `dtype`
- `shape`

Each projected attribute appends one column:

- `h5_attr()` defaults to output column name `h5_attr`
- `h5_attr(name)` defaults to output column name `name`
- `h5_attr(name, default_value)` defaults to output column name `name`
- `h5_alias(...)` overrides the default output name
- `h5_attr()` has type `MAP(VARCHAR, VARIANT)`
- `h5_attr(name)` has type `VARIANT`
- `h5_attr(name, default_value)` has type `type(default_value)`

Projected output names must be unique within the final result schema. If a
projected name duplicates another projected name, or collides with an existing
`h5_tree` column such as `path`, bind fails with DuckDB's duplicate-column
error.

## `h5_attr` Semantics

`h5_attr()` defines one projected all-attributes column.

Rules:

- output name defaults to `h5_attr`
- output type is `MAP(VARCHAR, VARIANT)`
- on resolved rows, all attributes are returned in one map keyed by attribute name
- if the object has no attributes, the result is an empty map
- on unresolved or external rows, the result is `NULL`
- unsupported attribute values are inserted as `NULL` map entries instead of failing the query

`h5_attr(name, default_value)` defines one projected attribute column.

Rules:

- `name` must resolve to a non-`NULL` `VARCHAR` value at bind time
- constant expressions such as `lower('STRING_ATTR')` are allowed
- row-dependent expressions are not allowed; DuckDB rejects them as unsupported
  lateral parameters for `h5_tree`
- `default_value` must be a bind-time constant expression
- `default_value` must have a concrete type
- typed `NULL` defaults such as `NULL::VARCHAR` are allowed
- untyped `NULL` defaults are rejected
- `h5_attr(name)` is shorthand for `h5_attr(name, NULL::VARIANT)`

Examples:

```sql
h5_attr()
h5_attr('NX_class', NULL::VARCHAR)
h5_attr(lower('STRING_ATTR'), NULL::VARCHAR)
h5_attr('count_time', 0::DOUBLE)
h5_attr('units', 'unknown'::VARCHAR)
```

## Row Semantics

For each projected attribute column:

- if the projection is `h5_attr()`:
  - iterate all attributes on the current object
  - convert each supported value to `VARIANT`
  - if one attribute value is unsupported, store that entry as `NULL`
  - emit the resulting map
- if the current object has the attribute:
  - read the attribute
  - convert it to a DuckDB value using the same conversion rules as
    `h5_attributes(...)`
  - cast it to the declared output type from `default_value`
  - emit the cast value
- if the current object does not have the attribute:
  - emit `default_value`

There is no separate "missing without default" case because single-attribute
projections always have an implicit or explicit default.

## Type and Error Behavior

Projected attributes reuse the same HDF5-to-DuckDB conversion rules as
`h5_attributes(...)`.

Current supported attribute forms:

- scalar numeric attributes
- scalar string attributes
- simple 1D numeric array attributes
- HDF5 `H5T_ARRAY` attributes when they are 1D and map to supported DuckDB types

Current unsupported forms include:

- multidimensional attribute dataspaces
- string array attributes
- the same unsupported HDF5 attribute types already rejected by `h5_attributes(...)`

UTF-8 behavior:

- invalid UTF-8 string values are preserved as `BLOB` in `VARIANT`-typed projected
  attributes and in `BLOB`-typed projections
- text-typed projected attributes still fail on invalid UTF-8 string values

Error behavior:

- invalid projected argument shape:
  - `h5_tree projected attribute arguments must be h5_attr(), h5_attr(name), h5_attr(name, default_value) or h5_alias(alias, h5_attr(...))`
- `NULL` attribute name:
  - `h5_attr name must not be NULL`
- non-constant default:
  - `h5_attr default_value must be a constant expression`
- untyped `NULL` default:
  - `h5_attr default_value must have a concrete type; use an explicit cast such as NULL::VARCHAR`
- cast failure:
  - `Attribute 'name' contains values that cannot be cast to <TYPE>`
- unsupported attribute forms:
  - the same error messages produced by `h5_attributes(...)`

For wrong-type names and lateral/column-dependent names, DuckDB's normal binder
errors are preserved.

## Execution Strategy

The implementation keeps bind schema-only.

Execution rules:

- bind parses projected attribute markers and fixes the output schema
- bind does not traverse the HDF5 file
- `h5_tree` performs a single namespace traversal in its worker thread
- `h5_tree` eagerly materializes `type`, dataset `dtype`, dataset `shape`, and
  projected attribute values for each emitted row during that traversal
- `h5_ls` performs the same metadata and projected-attribute work for one group's
  immediate children
- scan writes already-materialized row data into DuckDB output chunks

## Implementation Notes

The implementation currently lives in:

- `src/h5_tree.cpp`
- `src/h5_ls.cpp`
- `src/h5_tree_shared.cpp`
- `src/h5_attributes.cpp`
- `src/h5_common.cpp`
- `src/include/h5_functions.hpp`

Attribute projection and `h5_attributes(...)` share the same helper path for:

- resolving attribute DuckDB logical types
- reading HDF5 attribute values into DuckDB `Value`s
- enforcing current unsupported-form behavior
- applying the same projected-attribute semantics in both `h5_tree` and `h5_ls`

## Coverage

Focused SQLLogic coverage lives in:

- `test/sql/h5_tree_attributes.test`

The test coverage includes:

- all-attributes map projection with default column naming
- aliased all-attributes map projection
- `NULL` vs empty-map behavior for unresolved/external vs resolved rows
- unsupported attribute entries degrading to `NULL` inside the map
- scalar projected attributes with typed `NULL` defaults
- scalar projected attributes with non-`NULL` defaults
- array projected attributes
- aliased projected attributes
- multiple projected attributes with mixed present/missing rows
- duplicate projected output names
- collisions with existing `h5_tree` column names
- invalid projected argument shapes
- non-constant defaults
- untyped `NULL` defaults
- `NULL` attribute names
- wrong-type names and lateral-name binder errors
- cast failures
- unsupported attribute forms
- remote-path execution
