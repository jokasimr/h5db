# Notes for AI Agents Working on H5DB

## ⚠️ CRITICAL: Always Report Bugs You Discover

**NEVER silently work around bugs you discover during testing or development.**

If you find a bug:
1. **STOP** - Do not work around it or change the test to avoid it
2. **REPORT** - Explicitly tell the user what you found
3. **FIX** - Fix the bug properly
4. **TEST** - Verify the fix works

### Example of WRONG Behavior ❌
```
Test fails: WHERE col != 20 returns 0 rows (should return 800)
→ Change test to avoid != operator
→ Move on without reporting
```

### Example of CORRECT Behavior ✅
```
Test fails: WHERE col != 20 returns 0 rows (should return 800)
→ Report: "CRITICAL BUG: != operator doesn't work on RSE columns"
→ Investigate root cause (missing COMPARE_NOTEQUAL handling)
→ Fix the bug in TryClaimRSEFilter
→ Verify fix with tests
```

**The user needs to know about ALL bugs, even if you can work around them!**

---

## Critical: Always Use the Virtual Environment

**ALWAYS use the project's Python virtual environment for all Python commands.**

```bash
# Activate venv (includes all tools + build environment)
source venv/bin/activate

# Run commands
make format-check
make format
make tidy-check
make -j8
make test

# Deactivate when done
deactivate
```

The venv includes:
- Python packages (h5py, numpy)
- Code formatting tools (black, clang-format, cmake-format, clang-tidy)
- Build environment variables (VCPKG_TOOLCHAIN_PATH, GEN)

**NEVER use system Python/pip** (e.g., `python3`, `pip3`) for project tasks.

## Build Commands

```bash
source venv/bin/activate
make -j8    # Build
make test          # Run tests
```

## Code Quality Checks

Before committing:

```bash
source venv/bin/activate
make format-check  # Check formatting
make format        # Auto-fix formatting
make tidy-check    # Static analysis (slower)
```

## Common Pitfalls

1. **Don't use system Python** - Always activate venv first
2. **Don't forget to activate venv** - Everything needs it
3. **Don't commit without formatting** - Run `make format` before commits
4. **Don't modify DuckDB submodule** - Extension code is in `src/` only
5. **Check your current directory if confused** - If you get path errors or command failures, run `pwd` to verify you're in the correct directory. Most commands should be run from the project root (`/home/johannes/personal/h5db`)

## File Structure

```
src/          - Extension source code
test/sql/     - SQLLogicTest test files
test/data/    - HDF5 test files + Python generators
venv/         - Python virtual environment (USE THIS)
duckdb/       - Git submodule (don't modify)
```

## One-time Setup (Already Done)

If setting up a fresh clone:
```bash
./scripts/setup-dev-env.sh              # Setup environment
./scripts/install-pre-commit-hook.sh    # Install pre-commit hook (optional)
```
