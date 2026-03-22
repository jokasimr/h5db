#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

PORT="18080"
UNITTEST_BIN="$PROJECT_ROOT/build/release/test/unittest"
TEST_GLOB="*"
PREPEND_FILE="$PROJECT_ROOT/test/sql/remote_httpfs_prelude.sql"

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --port <port>             Port for local http server (default: 18080).
  --prepend-file <path>     Optional SQL prelude prepended to every rewritten test.
  --unittest-bin <path>     unittest binary path (default: build/release/test/unittest).
  --test-glob <glob>        SQLLogicTest glob (default: *).
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --port)
      PORT="$2"; shift 2 ;;
    --prepend-file)
      PREPEND_FILE="$2"; shift 2 ;;
    --unittest-bin)
      UNITTEST_BIN="$2"; shift 2 ;;
    --test-glob)
      TEST_GLOB="$2"; shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "Unknown arg: $1" >&2
      usage
      exit 1 ;;
  esac
done

bash "$PROJECT_ROOT/test/data/ensure_test_data.sh"

TMP_ROOT="$(mktemp -d "$PROJECT_ROOT/test/.remote_sql.XXXXXX")"
TMP_SQL="$TMP_ROOT/sql"
SERVER_PID=""
GENERATED_PREPEND="$TMP_ROOT/remote_httpfs_prelude.sql"
cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_ROOT"
}
trap cleanup EXIT

BASE_URL="http://127.0.0.1:${PORT}"
python3 "$PROJECT_ROOT/test/scripts/range_http_server.py" \
  --port "$PORT" \
  --directory "$PROJECT_ROOT/test/data" >/tmp/h5db_remote_http.log 2>&1 &
SERVER_PID=$!

# Wait until the local server is actually serving range requests.
READY=0
for _ in $(seq 1 50); do
  if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    break
  fi
  if python3 - "$PORT" <<'PY'
import sys
import urllib.request

port = sys.argv[1]
url = f"http://127.0.0.1:{port}/simple.h5"
req = urllib.request.Request(url, method="HEAD", headers={"Range": "bytes=0-99"})
try:
    with urllib.request.urlopen(req, timeout=0.2) as resp:
        code = getattr(resp, "status", resp.getcode())
        sys.exit(0 if code == 206 else 1)
except Exception:
    sys.exit(1)
PY
  then
    READY=1
    break
  fi
  sleep 0.1
done

if [[ "$READY" -ne 1 ]]; then
  echo "Failed to start range HTTP server on port $PORT" >&2
  if [[ -f /tmp/h5db_remote_http.log ]]; then
    tail -n 80 /tmp/h5db_remote_http.log >&2 || true
  fi
  exit 1
fi

REWRITE_ARGS=(
  --input-root "$PROJECT_ROOT/test/sql"
  --output-root "$TMP_SQL"
  --base-url "$BASE_URL"
)
if [[ -n "$PREPEND_FILE" ]]; then
  python3 - "$PREPEND_FILE" "$GENERATED_PREPEND" "$PROJECT_ROOT/build/release/repository" <<'PY'
import sys
from pathlib import Path

src = Path(sys.argv[1])
dst = Path(sys.argv[2])
repo_path = sys.argv[3].replace("'", "''")

text = src.read_text(encoding="utf-8")
text = text.replace("__LOCAL_EXTENSION_REPO__", repo_path)
dst.write_text(text, encoding="utf-8")
PY
  REWRITE_ARGS+=(--prepend-file "$GENERATED_PREPEND")
fi

python3 "$PROJECT_ROOT/test/scripts/rewrite_remote_sqllogictests.py" "${REWRITE_ARGS[@]}"

TMP_SQL_REL="${TMP_SQL#$PROJECT_ROOT/}"
"$UNITTEST_BIN" "$TMP_SQL_REL/${TEST_GLOB}"
