#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

HOST="127.0.0.1"
PORT="2222"
USERNAME="h5db"
PASSWORD="h5db"
UNITTEST_BIN="$PROJECT_ROOT/build/release/test/unittest"
TEST_GLOB="*"

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --host <host>             Host for local SFTP server (default: 127.0.0.1).
  --port <port>             Port for local SFTP server (default: 2222).
  --username <user>         Username for SFTP auth (default: h5db).
  --password <pass>         Password for SFTP auth (default: h5db).
  --unittest-bin <path>     unittest binary path (default: build/release/test/unittest).
  --test-glob <glob>        SQLLogicTest glob (default: *).
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      HOST="$2"; shift 2 ;;
    --port)
      PORT="$2"; shift 2 ;;
    --username)
      USERNAME="$2"; shift 2 ;;
    --password)
      PASSWORD="$2"; shift 2 ;;
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

TMP_ROOT="$(mktemp -d "$PROJECT_ROOT/test/.remote_sftp_sql.XXXXXX")"
TMP_SQL="$TMP_ROOT/sql"
KNOWN_HOSTS="$TMP_ROOT/known_hosts"
HOST_KEY="$TMP_ROOT/host_key"
PRELUDE="$TMP_ROOT/remote_sftp_prelude.sql"
SERVER_PID=""
cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_ROOT"
}
trap cleanup EXIT

BASE_URL="sftp://${HOST}:${PORT}"

cat >"$PRELUDE" <<SQL
statement ok
CREATE OR REPLACE TEMPORARY SECRET h5db_remote_sftp (
    TYPE sftp,
    SCOPE '${BASE_URL}',
    USERNAME '${USERNAME}',
    PASSWORD '${PASSWORD}',
    KNOWN_HOSTS_PATH '${KNOWN_HOSTS}',
    PORT ${PORT}
);
SQL

./venv/bin/python "$PROJECT_ROOT/test/scripts/sftp_test_server.py" \
  --host "$HOST" \
  --port "$PORT" \
  --directory "$PROJECT_ROOT/test/data" \
  --username "$USERNAME" \
  --password "$PASSWORD" \
  --host-key-file "$HOST_KEY" \
  --known-hosts-file "$KNOWN_HOSTS" >/tmp/h5db_remote_sftp.log 2>&1 &
SERVER_PID=$!

READY=0
for _ in $(seq 1 50); do
  if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    break
  fi
  if [[ -s "$KNOWN_HOSTS" ]] && python3 - "$HOST" "$PORT" <<'PY'
import socket
import sys

host = sys.argv[1]
port = int(sys.argv[2])
try:
    with socket.create_connection((host, port), timeout=0.2):
        sys.exit(0)
except OSError:
    sys.exit(1)
PY
  then
    READY=1
    break
  fi
  sleep 0.1
done

if [[ "$READY" -ne 1 ]]; then
  echo "Failed to start SFTP server on ${HOST}:${PORT}" >&2
  if [[ -f /tmp/h5db_remote_sftp.log ]]; then
    tail -n 80 /tmp/h5db_remote_sftp.log >&2 || true
  fi
  exit 1
fi

python3 "$PROJECT_ROOT/test/scripts/rewrite_remote_sqllogictests.py" \
  --input-root "$PROJECT_ROOT/test/sql" \
  --output-root "$TMP_SQL" \
  --base-url "$BASE_URL" \
  --exclude-subdir remote \
  --prepend-file "$PRELUDE" \
  --prepend-after-require

TMP_SQL_REL="${TMP_SQL#$PROJECT_ROOT/}"
"$UNITTEST_BIN" "$TMP_SQL_REL/${TEST_GLOB}"
