#!/bin/bash

set -euo pipefail

case "$(uname -s 2>/dev/null || printf unknown)" in
  MINGW*|MSYS*|CYGWIN*)
    ;;
  *)
    exit 0
    ;;
esac

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <h5fopen_symlink_smoke_binary>" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$PROJECT_ROOT"

binary="$1"
if [ ! -x "$binary" ] && [ -x "$binary.exe" ]; then
  binary="$binary.exe"
fi

if [ ! -x "$binary" ]; then
  echo "Direct H5Fopen smoke binary not found or not executable: $binary" >&2
  exit 2
fi

echo "Running direct H5Fopen symlink smoke test"
"$binary" \
  test/data/glob_symlink/real/nested.h5 \
  test/data/glob_symlink/link_file.h5
