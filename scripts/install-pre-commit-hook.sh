#!/bin/bash
# Install pre-commit hook for format checking

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
HOOK_FILE="$PROJECT_ROOT/.git/hooks/pre-commit"

if [ -f "$HOOK_FILE" ]; then
    backup="$HOOK_FILE.bak.$(date +%Y%m%d%H%M%S)"
    cp "$HOOK_FILE" "$backup"
    echo "Existing hook backed up to $backup"
fi

cat > "$HOOK_FILE" << 'EOF'
#!/bin/bash
# H5DB Pre-commit Hook - Format Check

set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
LOG_FILE="$(mktemp "${TMPDIR:-/tmp}/h5db-format-check.XXXXXX")"
trap 'rm -f "$LOG_FILE"' EXIT

if [ -d "$ROOT/venv/bin" ]; then
    export PATH="$ROOT/venv/bin:$PATH"
fi

echo "Running format-check..."

# Run format check
if ! make -C "$ROOT" format-check > "$LOG_FILE" 2>&1; then
    echo "❌ Format check FAILED!"
    echo ""
    tail -15 "$LOG_FILE"
    echo ""
    echo "To fix: ./scripts/setup-dev-env.sh (if needed), then make format"
    echo "Then stage the changes and commit again."
    exit 1
fi

echo "✅ Format check passed"
exit 0
EOF

chmod +x "$HOOK_FILE"

echo "✅ Pre-commit hook installed at .git/hooks/pre-commit"
echo ""
echo "The hook will run 'make format-check' before each commit."
echo "If formatting issues are found, the commit will be blocked."
echo ""
echo "To fix formatting issues: ./scripts/setup-dev-env.sh (if needed), then make format"
