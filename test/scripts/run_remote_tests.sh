#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

PORT="18080"
UNITTEST_BIN="$PROJECT_ROOT/build/release/test/unittest"
TEST_GLOB="*"
PREPEND_FILE="$PROJECT_ROOT/test/sql/remote_httpfs_prelude.sql"
PORT_TRIES=10

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
cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_ROOT"
}
trap cleanup EXIT

BASE_URL="http://127.0.0.1:${PORT}"
READY=0
LAST_LOG="$TMP_ROOT/h5db_remote_http.log"
for PORT_OFFSET in $(seq 0 $((PORT_TRIES - 1))); do
  CANDIDATE_PORT=$((PORT + PORT_OFFSET))
  LAST_LOG="$TMP_ROOT/h5db_remote_http.${CANDIDATE_PORT}.log"

  python3 "$PROJECT_ROOT/test/scripts/range_http_server.py" \
    --port "$CANDIDATE_PORT" \
    --directory "$PROJECT_ROOT/test/data" >"$LAST_LOG" 2>&1 &
  SERVER_PID=$!

  for _ in $(seq 1 50); do
    if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
      break
    fi
    if python3 - "$CANDIDATE_PORT" <<'PY'
import sys
import urllib.request

port = int(sys.argv[1])
url = f"http://127.0.0.1:{port}/simple.h5"
req = urllib.request.Request(url, method="HEAD", headers={"Range": "bytes=0-99"})
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
try:
    with opener.open(req, timeout=0.5) as resp:
        code = getattr(resp, "status", resp.getcode())
        sys.exit(0 if code == 206 else 1)
except Exception:
    sys.exit(1)
PY
    then
      READY=1
      PORT="$CANDIDATE_PORT"
      BASE_URL="http://127.0.0.1:${PORT}"
      break 2
    fi
    sleep 0.1
  done

  kill "$SERVER_PID" >/dev/null 2>&1 || true
  wait "$SERVER_PID" >/dev/null 2>&1 || true
  SERVER_PID=""
done

if [[ "$READY" -ne 1 ]]; then
  echo "Failed to start range HTTP server on ports ${PORT}-$((PORT + PORT_TRIES - 1))" >&2
  if [[ -f "$LAST_LOG" ]]; then
    tail -n 80 "$LAST_LOG" >&2 || true
  fi
  exit 1
fi

REWRITE_ARGS=(
  --input-root "$PROJECT_ROOT/test/sql"
  --output-root "$TMP_SQL"
  --base-url "$BASE_URL"
)
if [[ -n "$PREPEND_FILE" ]]; then
  REWRITE_ARGS+=(--prepend-file "$PREPEND_FILE")
fi

python3 "$PROJECT_ROOT/test/scripts/rewrite_remote_sqllogictests.py" "${REWRITE_ARGS[@]}"

TMP_SQL_REL="${TMP_SQL#$PROJECT_ROOT/}"
"$UNITTEST_BIN" "$TMP_SQL_REL/${TEST_GLOB}"
