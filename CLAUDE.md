# Notes for AI Agents Working on H5DB

## Critical: Always Use the Virtual Environment

**ALWAYS use the project's Python virtual environment for all Python commands.**

```bash
# Activate venv (includes all tools + build environment)
source venv/bin/activate

# Run commands
make format-check
make format
make tidy-check
make -j$(nproc)
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
make -j$(nproc)    # Build
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
