#!/bin/bash
# H5DB Development Environment Setup Script
#
# This script sets up the Python virtual environment with all necessary
# development tools and configures it to work with the project's build system.
#
# Usage:
#   ./scripts/setup-dev-env.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
VENV_DIR="$PROJECT_ROOT/venv"

echo "==> Setting up h5db development environment"

# Check if venv exists, create if not
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv "$VENV_DIR"
else
    echo "Virtual environment already exists"
fi

# Activate venv
source "$VENV_DIR/bin/activate"

echo "Installing Python packages..."

# Install test data generation tools
pip install -q h5py numpy

# Install code formatting tools (required by CI)
pip install -q "black>=24" clang-format==11.0.1 cmake-format clang-tidy

echo "Configuring venv activation script..."

# Add .env sourcing to venv activate script
# This ensures VCPKG_TOOLCHAIN_PATH and GEN are set when venv is activated
ACTIVATE_SCRIPT="$VENV_DIR/bin/activate"

# Check if .env sourcing is already in activate script
if ! grep -q "H5DB project environment" "$ACTIVATE_SCRIPT"; then
    cat >> "$ACTIVATE_SCRIPT" << 'EOF'

# H5DB project environment variables
if [ -f "${VIRTUAL_ENV}/../.env" ]; then
    source "${VIRTUAL_ENV}/../.env"
fi
EOF
    echo "Added .env sourcing to venv activate script"
else
    echo ".env sourcing already configured"
fi

deactivate

echo ""
echo "✅ Development environment setup complete!"
echo ""
echo "To activate the environment:"
echo "  source venv/bin/activate"
echo ""
echo "This will give you access to:"
echo "  - Python packages (h5py, numpy)"
echo "  - Formatting tools (black, clang-format, cmake-format, clang-tidy)"
echo "  - Build environment variables (VCPKG_TOOLCHAIN_PATH, GEN)"
echo ""
echo "Then you can run:"
echo "  make format-check    # Check code formatting"
echo "  make format          # Fix code formatting"
echo "  make tidy-check      # Run clang-tidy static analysis"
echo "  make -j\$(nproc)      # Build the extension"
echo "  make test            # Run tests"
