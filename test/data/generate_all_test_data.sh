#!/bin/bash
#
# Generate all test data files for h5db test suite
#
# This script regenerates all .h5/.hdf data files used by the test suite.
# It does NOT generate benchmark files (those are optional).
#

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LARGE_DATA_DIR="$PROJECT_ROOT/test/data/large"
PYTHON_BIN="python3"

if ! command -v python3 >/dev/null 2>&1; then
    if command -v python >/dev/null 2>&1; then
        PYTHON_BIN="python"
    else
        echo -e "${RED}Error: python not found${NC}"
        exit 1
    fi
fi

echo "==================================================================="
echo "Generating all test data files"
echo "==================================================================="
echo ""

# Check for virtual environment (optional)
if [ -z "$VIRTUAL_ENV" ]; then
    if [ -f "$PROJECT_ROOT/venv/bin/activate" ]; then
        echo -e "${YELLOW}Activating virtual environment...${NC}"
        source "$PROJECT_ROOT/venv/bin/activate"
    elif [ -f "$PROJECT_ROOT/venv/Scripts/activate" ]; then
        echo -e "${YELLOW}Activating virtual environment...${NC}"
        source "$PROJECT_ROOT/venv/Scripts/activate"
    fi
fi

# Check for h5py
if ! "$PYTHON_BIN" -c "import h5py" 2>/dev/null; then
    echo -e "${YELLOW}h5py not found, attempting to install...${NC}"
    if ! "$PYTHON_BIN" -m pip --version >/dev/null 2>&1; then
        echo -e "${YELLOW}pip not found, bootstrapping with ensurepip...${NC}"
        "$PYTHON_BIN" -m ensurepip --upgrade >/dev/null 2>&1 || true
    fi
    if "$PYTHON_BIN" -m pip --version >/dev/null 2>&1; then
        "$PYTHON_BIN" -m pip install --user h5py >/dev/null
    elif command -v apk >/dev/null 2>&1; then
        echo -e "${YELLOW}pip still unavailable, installing via apk...${NC}"
        apk add --no-cache py3-pip py3-h5py >/dev/null
    fi
    if ! "$PYTHON_BIN" -c "import h5py" 2>/dev/null; then
        echo -e "${RED}Error: h5py not found${NC}"
        echo "Install with: python -m pip install --user h5py"
        echo "Or on Alpine: apk add --no-cache py3-pip py3-h5py"
        exit 1
    fi
fi

cd "$PROJECT_ROOT"

# ====================================================================
# Core test data files (test/data/)
# ====================================================================
echo -e "${GREEN}[1/25] Generating core test files (simple.h5, types.h5, multidim.h5)${NC}"
(cd test/data && "$PYTHON_BIN" create_simple_types_multidim.py)

echo ""
echo -e "${GREEN}[2/25] Generating run_encoded.h5${NC}"
(cd test/data && "$PYTHON_BIN" create_run_encoded_test.py)

echo ""
echo -e "${GREEN}[3/25] Generating with_attrs.h5${NC}"
(cd test/data && "$PYTHON_BIN" create_attrs_test.py)

echo ""
echo -e "${GREEN}[4/25] Generating multithreading_test.h5${NC}"
(cd test/data && "$PYTHON_BIN" create_multithreading_test.py)

echo ""
echo -e "${GREEN}[5/25] Generating pushdown_test.h5${NC}"
(cd test/data && "$PYTHON_BIN" create_pushdown_test.py)

echo ""
echo -e "${GREEN}[6/25] Generating rse_edge_cases.h5${NC}"
(cd test/data && "$PYTHON_BIN" create_rse_edge_cases.py)

echo ""
echo -e "${GREEN}[7/25] Generating rse_invalid.h5${NC}"
(cd test/data && "$PYTHON_BIN" create_rse_invalid_test.py)

echo ""
echo -e "${GREEN}[8/25] Generating unsupported_types.h5${NC}"
(cd test/data && "$PYTHON_BIN" create_unsupported_types_test.py)

echo ""
echo -e "${GREEN}[9/25] Generating attrs_edge_cases.h5${NC}"
(cd test/data && "$PYTHON_BIN" create_attrs_edge_cases.py)

echo ""
echo -e "${GREEN}[10/25] Generating root_attrs.h5${NC}"
(cd test/data && "$PYTHON_BIN" create_root_attrs_test.py)

echo ""
echo -e "${GREEN}[11/25] Generating empty_scalar.h5${NC}"
(cd test/data && "$PYTHON_BIN" create_empty_scalar_test.py)

echo ""
echo -e "${GREEN}[12/25] Generating names_edge_cases.h5${NC}"
(cd test/data && "$PYTHON_BIN" create_names_edge_cases.py)

echo ""
echo -e "${GREEN}[13/25] Generating multidim_mismatch.h5${NC}"
(cd test/data && "$PYTHON_BIN" create_multidim_mismatch_test.py)

echo -e "${GREEN}[14/25] Generating many_groups.h5${NC}"
(cd test/data && "$PYTHON_BIN" create_many_groups_test.py)

echo ""
echo -e "${GREEN}[15/25] Generating nd_cache_test.h5${NC}"
(cd test/data && "$PYTHON_BIN" create_nd_cache_test.py)

echo ""
echo -e "${GREEN}[16/25] Generating cache_boundaries.h5${NC}"
(cd test/data && "$PYTHON_BIN" create_cache_boundaries_test.py)

echo ""
echo -e "${GREEN}[17/25] Generating sparse_pushdown_cache.h5${NC}"
(cd test/data && "$PYTHON_BIN" create_sparse_pushdown_cache_test.py)

echo ""
echo -e "${GREEN}[18/25] Generating wide_few_rows.h5${NC}"
(cd test/data && "$PYTHON_BIN" create_wide_few_rows_test.py)

echo ""
echo -e "${GREEN}[19/25] Generating links.h5${NC}"
(cd test/data && "$PYTHON_BIN" create_links_test.py)

echo ""
echo -e "${GREEN}[20/25] Generating swmr test files${NC}"
(cd test/data && "$PYTHON_BIN" create_swmr_test.py)

# ====================================================================
# Large test data files (test/data/large/)
# ====================================================================
mkdir -p "$LARGE_DATA_DIR"
echo ""
echo -e "${GREEN}[21/25] Generating large_rse_test.h5 (16 MB)${NC}"
(cd test/data && "$PYTHON_BIN" create_large_rse_test.py)

echo ""
echo -e "${GREEN}[22/25] Generating large_simple.h5 (1.3 GB)${NC}"
(cd "$LARGE_DATA_DIR" && "$PYTHON_BIN" "$PROJECT_ROOT/test/data/large/create_large_simple.py")

echo ""
echo -e "${GREEN}[23/25] Generating large_multithreading.h5 (153 MB)${NC}"
(cd "$LARGE_DATA_DIR" && "$PYTHON_BIN" "$PROJECT_ROOT/test/data/large/create_large_multithreading.py")

echo ""
echo -e "${GREEN}[24/25] Generating large_pushdown_test.h5 (115 MB)${NC}"
(cd "$LARGE_DATA_DIR" && "$PYTHON_BIN" "$PROJECT_ROOT/test/data/large/create_large_pushdown.py")

echo ""
echo -e "${GREEN}[25/25] Generating large_rse_edge_cases.h5 (266 MB)${NC}"
(cd "$LARGE_DATA_DIR" && "$PYTHON_BIN" "$PROJECT_ROOT/test/data/large/create_large_rse_edge_cases.py")

# ====================================================================
# Summary
# ====================================================================
echo ""
echo "==================================================================="
echo -e "${GREEN}OK All test data files generated successfully!${NC}"
echo "==================================================================="
echo ""
echo "Generated files:"
echo "  Core tests (test/data/):"
echo "    - simple.h5               (basic datasets)"
echo "    - types.h5                (type system tests)"
echo "    - multidim.h5             (multi-dimensional arrays)"
echo "    - run_encoded.h5          (RSE functionality)"
echo "    - with_attrs.h5           (HDF5 attributes)"
echo "    - multithreading_test.h5  (parallel execution)"
echo "    - pushdown_test.h5        (predicate pushdown)"
echo "    - rse_edge_cases.h5       (RSE edge cases)"
echo "    - rse_invalid.h5          (RSE validation errors)"
echo "    - unsupported_types.h5    (unsupported HDF5 types)"
echo "    - attrs_edge_cases.h5     (attribute edge cases)"
echo "    - root_attrs.h5           (root attributes + string-array attr edge case)"
echo "    - empty_scalar.h5         (empty + scalar datasets)"
echo "    - names_edge_cases.h5     (dataset name edge cases)"
echo "    - multidim_mismatch.h5    (multidim row-count mismatch)"
echo "    - nd_cache_test.h5        (N-D cache coverage)"
echo "    - cache_boundaries.h5     (regular cache row-count boundaries)"
echo "    - sparse_pushdown_cache.h5 (sparse pushdown cache coverage)"
echo "    - wide_few_rows.h5        (wide-row cache/threading coverage)"
echo "    - links.h5                (hard/soft link coverage)"
echo "    - swmr_enabled.h5         (SWMR flag enabled)"
echo "    - swmr_disabled.h5        (SWMR flag disabled)"
echo "    - large_rse_test.h5       (RSE multithreading regression)"
echo ""
echo "  Large tests (test/data/large/):"
echo "    - large_simple.h5             (1.3 GB)"
echo "    - large_multithreading.h5     (153 MB)"
echo "    - large_pushdown_test.h5      (115 MB)"
echo "    - large_rse_edge_cases.h5     (266 MB)"
echo ""
echo "  Total size: ~2.2 GB"
echo ""
echo "Run tests with: make test"
