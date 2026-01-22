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
  "$PROJECT_ROOT/test/data/empty_scalar.h5"
  "$PROJECT_ROOT/test/data/names_edge_cases.h5"
  "$PROJECT_ROOT/test/data/multidim_mismatch.h5"
  "$PROJECT_ROOT/test/data/nd_cache_test.h5"
  "$PROJECT_ROOT/test/data/large_rse_test.h5"
  "$PROJECT_ROOT/test/data/large/large_simple.h5"
  "$PROJECT_ROOT/test/data/large/large_multithreading.h5"
  "$PROJECT_ROOT/test/data/large/large_pushdown_test.h5"
  "$PROJECT_ROOT/test/data/large/large_rse_edge_cases.h5"
)

missing=()
for file in "${required_files[@]}"; do
  if [ ! -f "$file" ]; then
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
