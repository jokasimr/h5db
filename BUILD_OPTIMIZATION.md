# Build Optimization Guide

This document explains how to significantly speed up DuckDB extension builds.

## Problem

DuckDB extensions build the entire DuckDB core library, which takes 10-20 minutes on the first build. Without optimization, subsequent builds unnecessarily recompile unchanged code.

## Solution: ccache + ninja

Two tools dramatically improve build performance:

1. **ccache** - Compiler cache that avoids recompiling unchanged files
2. **ninja** - Fast build system that replaces Make with better parallelization

### Installation

#### Ubuntu/Debian
```bash
sudo apt-get update
sudo apt-get install ccache ninja-build
```

#### Fedora/RHEL
```bash
sudo dnf install ccache ninja-build
```

#### macOS (Homebrew)
```bash
brew install ccache ninja
```

#### Arch Linux
```bash
sudo pacman -S ccache ninja
```

### Usage

After installing both tools, use this command instead of `make`:

```bash
GEN=ninja make
```

This tells CMake to use ninja as the generator while leveraging ccache automatically.

### Performance Impact

**Without optimization:**
- First build: ~15-20 minutes
- Subsequent builds: ~15-20 minutes (full recompile)

**With ccache + ninja:**
- First build: ~10-15 minutes (ninja parallelization)
- Subsequent builds: ~30 seconds - 2 minutes (only changed files)
- Extension-only changes: ~10-30 seconds

### Verification

Check if tools are working:

```bash
# Verify installation
which ccache
which ninja

# Check ccache statistics
ccache -s

# After a build, check what's cached
ccache -s
```

### Best Practices

1. **Always use `GEN=ninja make`** for all builds after installing the tools
2. **Clean builds**: When switching branches significantly, run `make clean` first
3. **ccache size**: Default cache is 5GB. Increase if needed:
   ```bash
   ccache --max-size=10G
   ```
4. **Parallel jobs**: ninja auto-detects CPU cores. Override if needed:
   ```bash
   GEN=ninja make -j8
   ```

### Development Workflow

**Initial setup (once):**
```bash
# Install tools
sudo apt-get install ccache ninja-build

# First build (slower)
GEN=ninja make
```

**Daily development:**
```bash
# Edit source files
# ...

# Fast rebuild (only changed files)
GEN=ninja make

# Run tests
make test
```

**Clean rebuild (when needed):**
```bash
make clean
GEN=ninja make
```

## Additional Optimizations

### Debug vs Release Builds

- **Release builds** (default): Optimized, slower to compile
- **Debug builds**: Faster to compile, useful for development

To use debug builds during development:
```bash
BUILD_DEBUG=1 GEN=ninja make
```

### Minimal Builds

For rapid iteration on extension code only (without rebuilding DuckDB):
```bash
# Build only the extension (after DuckDB is built once)
GEN=ninja make extension
```

Note: This only works after a full build has completed at least once.

## Troubleshooting

### ccache not being used
Check if ccache is in your PATH and properly configured:
```bash
ccache --version
ccache -p  # Show configuration
```

### Ninja errors
If ninja fails, fall back to Make temporarily:
```bash
make  # Uses default Make instead of ninja
```

### Clear ccache (nuclear option)
If you suspect cache corruption:
```bash
ccache --clear
```

## Current Status

- ❌ ccache: NOT INSTALLED
- ❌ ninja: NOT INSTALLED
- Current build method: `make` (default, slow)
- Recommended: Install both tools and use `GEN=ninja make`

## References

- [DuckDB Extension Template](https://github.com/duckdb/extension-template)
- [ccache documentation](https://ccache.dev/)
- [Ninja build system](https://ninja-build.org/)
