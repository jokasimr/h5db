#!/bin/bash
# Install pre-commit hook for format checking

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
HOOK_FILE="$PROJECT_ROOT/.git/hooks/pre-commit"

cat > "$HOOK_FILE" << 'EOF'
#!/bin/bash
# H5DB Pre-commit Hook - Format Check

echo "Running format-check..."

# Activate venv (which sources .env automatically)
source "$(git rev-parse --show-toplevel)/venv/bin/activate"

# Run format check
if ! make format-check > /tmp/format-check.log 2>&1; then
    echo "❌ Format check FAILED!"
    echo ""
    tail -15 /tmp/format-check.log
    echo ""
    echo "To fix: source venv/bin/activate && make format"
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
echo "To fix formatting issues: source venv/bin/activate && make format"
