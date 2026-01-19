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

echo "==================================================================="
echo "Generating all test data files"
echo "==================================================================="
echo ""

# Check for virtual environment (optional)
if [ -z "$VIRTUAL_ENV" ] && [ -f "$PROJECT_ROOT/venv/bin/activate" ]; then
    echo -e "${YELLOW}Activating virtual environment...${NC}"
    source "$PROJECT_ROOT/venv/bin/activate"
fi

# Check for h5py
if ! python3 -c "import h5py" 2>/dev/null; then
    echo -e "${YELLOW}h5py not found, attempting to install...${NC}"
    if ! python3 -m pip --version >/dev/null 2>&1; then
        echo -e "${YELLOW}pip not found, bootstrapping with ensurepip...${NC}"
        python3 -m ensurepip --upgrade >/dev/null 2>&1 || true
    fi
    if python3 -m pip --version >/dev/null 2>&1; then
        python3 -m pip install --user h5py >/dev/null
    elif command -v apk >/dev/null 2>&1; then
        echo -e "${YELLOW}pip still unavailable, installing via apk...${NC}"
        apk add --no-cache py3-pip py3-h5py >/dev/null
    fi
    if ! python3 -c "import h5py" 2>/dev/null; then
        echo -e "${RED}Error: h5py not found${NC}"
        echo "Install with: python3 -m pip install --user h5py"
        echo "Or on Alpine: apk add --no-cache py3-pip py3-h5py"
        exit 1
    fi
fi

cd "$PROJECT_ROOT"

# ====================================================================
# Core test data files (test/data/)
# ====================================================================
echo -e "${GREEN}[1/12] Generating core test files (simple.h5, types.h5, multidim.h5)${NC}"
(cd test/data && python3 create_simple_types_multidim.py)

echo ""
echo -e "${GREEN}[2/12] Generating run_encoded.h5${NC}"
(cd test/data && python3 create_run_encoded_test.py)

echo ""
echo -e "${GREEN}[3/12] Generating with_attrs.h5${NC}"
(cd test/data && python3 create_attrs_test.py)

echo ""
echo -e "${GREEN}[4/12] Generating multithreading_test.h5${NC}"
(cd test/data && python3 create_multithreading_test.py)

echo ""
echo -e "${GREEN}[5/12] Generating pushdown_test.h5${NC}"
(cd test/data && python3 create_pushdown_test.py)

echo ""
echo -e "${GREEN}[6/12] Generating rse_edge_cases.h5${NC}"
(cd test/data && python3 create_rse_edge_cases.py)

echo ""
echo -e "${GREEN}[7/12] Generating nd_cache_test.h5${NC}"
(cd test/data && python3 create_nd_cache_test.py)

# ====================================================================
# Large test data files (test/data/large/)
# ====================================================================
mkdir -p "$LARGE_DATA_DIR"
echo ""
echo -e "${GREEN}[8/12] Generating large_rse_test.h5 (16 MB)${NC}"
(cd test/data && python3 create_large_rse_test.py)

echo ""
echo -e "${GREEN}[9/12] Generating large_simple.h5 (1.3 GB)${NC}"
(cd "$LARGE_DATA_DIR" && python3 "$PROJECT_ROOT/test/data/large/create_large_simple.py")

echo ""
echo -e "${GREEN}[10/12] Generating large_multithreading.h5 (153 MB)${NC}"
(cd "$LARGE_DATA_DIR" && python3 "$PROJECT_ROOT/test/data/large/create_large_multithreading.py")

echo ""
echo -e "${GREEN}[11/12] Generating large_pushdown_test.h5 (115 MB)${NC}"
(cd "$LARGE_DATA_DIR" && python3 "$PROJECT_ROOT/test/data/large/create_large_pushdown.py")

echo ""
echo -e "${GREEN}[12/12] Generating large_rse_edge_cases.h5 (266 MB)${NC}"
(cd "$LARGE_DATA_DIR" && python3 "$PROJECT_ROOT/test/data/large/create_large_rse_edge_cases.py")

# ====================================================================
# Summary
# ====================================================================
echo ""
echo "==================================================================="
echo -e "${GREEN}✅ All test data files generated successfully!${NC}"
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
echo "    - nd_cache_test.h5        (N-D cache coverage)"
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
