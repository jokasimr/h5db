#!/bin/bash
#
# Ensure all test data files exist. Generate them if any are missing.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

required_files=(
  "$PROJECT_ROOT/test/data/simple.h5"
  "$PROJECT_ROOT/test/data/types.h5"
  "$PROJECT_ROOT/test/data/multidim.h5"
  "$PROJECT_ROOT/test/data/run_encoded.h5"
  "$PROJECT_ROOT/test/data/with_attrs.h5"
  "$PROJECT_ROOT/test/data/multithreading_test.h5"
  "$PROJECT_ROOT/test/data/pushdown_test.h5"
  "$PROJECT_ROOT/test/data/rse_edge_cases.h5"
  "$PROJECT_ROOT/test/data/rse_invalid.h5"
  "$PROJECT_ROOT/test/data/unsupported_types.h5"
  "$PROJECT_ROOT/test/data/attrs_edge_cases.h5"
  "$PROJECT_ROOT/test/data/invalid_string_array_attrs.h5"
  "$PROJECT_ROOT/test/data/root_attrs.h5"
  "$PROJECT_ROOT/test/data/empty_scalar.h5"
  "$PROJECT_ROOT/test/data/h5_read_string_edge_cases.h5"
  "$PROJECT_ROOT/test/data/h5_read_refresh_order.h5"
  "$PROJECT_ROOT/test/data/names_edge_cases.h5"
  "$PROJECT_ROOT/test/data/multidim_mismatch.h5"
  "$PROJECT_ROOT/test/data/nd_cache_test.h5"
  "$PROJECT_ROOT/test/data/cache_boundaries.h5"
  "$PROJECT_ROOT/test/data/partition_ownership.h5"
  "$PROJECT_ROOT/test/data/sparse_pushdown_cache.h5"
  "$PROJECT_ROOT/test/data/sparse_partition_pushdown.h5"
  "$PROJECT_ROOT/test/data/wide_few_rows.h5"
  "$PROJECT_ROOT/test/data/links.h5"
  "$PROJECT_ROOT/test/data/complex_links.h5"
  "$PROJECT_ROOT/test/data/h5_tree_traversal_hint_bug.h5"
  "$PROJECT_ROOT/test/data/links_external_target.h5"
  "$PROJECT_ROOT/test/data/swmr_enabled.h5"
  "$PROJECT_ROOT/test/data/swmr_disabled.h5"
  "$PROJECT_ROOT/test/data/glob/glob_same_1.h5"
  "$PROJECT_ROOT/test/data/glob/glob_same_2.h5"
  "$PROJECT_ROOT/test/data/glob/nested/glob_same_3.h5"
  "$PROJECT_ROOT/test/data/glob/glob_mismatch.h5"
  "$PROJECT_ROOT/test/data/glob/glob_strings_1_ascii.h5"
  "$PROJECT_ROOT/test/data/glob/glob_strings_2_utf8.h5"
  "$PROJECT_ROOT/test/data/glob/rse_same_1.h5"
  "$PROJECT_ROOT/test/data/glob/rse_same_2.h5"
  "$PROJECT_ROOT/test/data/glob_pushdown/pushdown_1.h5"
  "$PROJECT_ROOT/test/data/glob_pushdown/pushdown_2.h5"
  "$PROJECT_ROOT/test/data/glob_pushdown/nested/pushdown_3.h5"
  "$PROJECT_ROOT/test/data/glob/glob_with_attrs_1.h5"
  "$PROJECT_ROOT/test/data/glob/glob_with_attrs_2.h5"
  "$PROJECT_ROOT/test/data/glob/attr_schema_1.h5"
  "$PROJECT_ROOT/test/data/glob/attr_schema_2.h5"
  "$PROJECT_ROOT/test/data/glob/attr_schema_name_mismatch.h5"
  "$PROJECT_ROOT/test/data/glob/attr_schema_type_mismatch.h5"
  "$PROJECT_ROOT/test/data/glob/glob_attrs_edge_1.h5"
  "$PROJECT_ROOT/test/data/glob/glob_attrs_edge_2.h5"
  "$PROJECT_ROOT/test/data/glob/equiv/equiv_1.h5"
  "$PROJECT_ROOT/test/data/glob/equiv/equiv_2.h5"
  "$PROJECT_ROOT/test/data/glob/equiv/nested/equiv_3.h5"
  "$PROJECT_ROOT/test/data/glob_symlink/root_file.h5"
  "$PROJECT_ROOT/test/data/glob_symlink/real/nested.h5"
  "$PROJECT_ROOT/test/data/glob_symlink/link_file.h5"
  "$PROJECT_ROOT/test/data/glob_symlink/broken_link.h5"
  "$PROJECT_ROOT/test/data/glob_symlink/link_dir/nested.h5"
  "$PROJECT_ROOT/test/data/glob_order/order_1.h5"
  "$PROJECT_ROOT/test/data/glob_order/order_2.h5"
  "$PROJECT_ROOT/test/data/glob_order/order_10.h5"
  "$PROJECT_ROOT/test/data/glob_order/nested/order_0.h5"
  "$PROJECT_ROOT/test/data/glob_order/z/order_99.h5"
  "$PROJECT_ROOT/test/data/glob_deep/deep_0.h5"
  "$PROJECT_ROOT/test/data/glob_deep/alpha/deep_1.h5"
  "$PROJECT_ROOT/test/data/glob_deep/alpha/beta/gamma/deep_2.h5"
  "$PROJECT_ROOT/test/data/glob_deep/alpha/beta/gamma/delta/epsilon/deep_3.h5"
  "$PROJECT_ROOT/test/data/glob_hidden/visible.h5"
  "$PROJECT_ROOT/test/data/glob_hidden/.hidden_root.h5"
  "$PROJECT_ROOT/test/data/glob_hidden/.hidden_dir/in_hidden_dir.h5"
  "$PROJECT_ROOT/test/data/glob_hidden/visible_dir/inside_visible.h5"
  "$PROJECT_ROOT/test/data/glob_hidden/visible_dir/.nested_hidden/in_nested_hidden.h5"
  "$PROJECT_ROOT/test/data/glob_hidden/.hidden_parent/visible_child/in_hidden_parent.h5"
  "$PROJECT_ROOT/test/data/glob_literal_meta/literal[1].h5"
  "$PROJECT_ROOT/test/data/glob_literal_meta/dir[1]/nested.h5"
  "$PROJECT_ROOT/test/data/glob_large/large_same_1.h5"
  "$PROJECT_ROOT/test/data/glob_large/large_same_2.h5"
  "$PROJECT_ROOT/test/data/glob_large/large_order_1.h5"
  "$PROJECT_ROOT/test/data/glob_large/large_order_2.h5"
  "$PROJECT_ROOT/test/data/glob_large/large_order_10.h5"
  "$PROJECT_ROOT/test/data/glob_many_small/combined.h5"
  "$PROJECT_ROOT/test/data/glob_many_small/part_0001.h5"
  "$PROJECT_ROOT/test/data/glob_many_small/part_1000.h5"
  "$PROJECT_ROOT/test/data/large_rse_test.h5"
  "$PROJECT_ROOT/test/data/large/large_simple.h5"
  "$PROJECT_ROOT/test/data/large/large_multithreading.h5"
  "$PROJECT_ROOT/test/data/large/large_pushdown_test.h5"
  "$PROJECT_ROOT/test/data/large/large_rse_edge_cases.h5"
  "$PROJECT_ROOT/test/data/many_groups.h5"
)

missing=()
for file in "${required_files[@]}"; do
  if [ ! -f "$file" ] && [ ! -L "$file" ]; then
    missing+=("$file")
  fi
done

if [ "${#missing[@]}" -eq 0 ]; then
  echo "All test data files are present."
  exit 0
fi

echo "Missing test data files:"
for file in "${missing[@]}"; do
  echo "  - $file"
done

echo "Generating test data..."
exec "$PROJECT_ROOT/test/data/generate_all_test_data.sh"
