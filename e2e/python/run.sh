#!/usr/bin/env bash
# Run the boto3 e2e suite under BOTH SQS wire protocols (AWS JSON 1.0 and legacy Query).
#
# Env knobs:
#   QD_E2E_ENDPOINT     use an already-running endpoint (no server is started; sigv4 tests skip)
#   QD_E2E_TARGET=aws   run against real AWS SQS (no server started; qd_only tests skip)
#   QD_E2E_BINARY       server binary to test (default: build target/debug/qd with cargo)
#   DATABASE_URL        admin Postgres URL used to create/drop the throwaway DB
#                       (default postgres://qd:qd@localhost/postgres)
#   QD_E2E_VENV_JSON    venv for the modern SDK   (default e2e/python/.venv-json)
#   QD_E2E_VENV_QUERY   venv for the legacy SDK   (default e2e/python/.venv-query)
#   QD_E2E_PROTOCOLS    which to run (default "json query")
#   QD_E2E_CLI=0        skip the AWS CLI smoke script (run by default when `aws` is on PATH)
#   PYTHON              interpreter used to create venvs (default python3)
# Extra arguments are passed to pytest, e.g. ./run.sh -k fifo -x
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
PY="${PYTHON:-python3}"
VENV_JSON="${QD_E2E_VENV_JSON:-$HERE/.venv-json}"
VENV_QUERY="${QD_E2E_VENV_QUERY:-$HERE/.venv-query}"
PROTOCOLS="${QD_E2E_PROTOCOLS:-json query}"
ADMIN_URL="${DATABASE_URL:-postgres://qd:qd@localhost/postgres}"
OUT="${QD_E2E_OUT:-$HERE/.out}"
mkdir -p "$OUT"

log() { printf '\033[1;34m[e2e]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31m[e2e] %s\033[0m\n' "$*" >&2; exit 2; }

ensure_venv() { # dir requirements-file
  local dir="$1" req="$2" stamp="$1/.req-stamp"
  if [[ ! -x "$dir/bin/python" ]]; then
    log "creating venv $dir"
    "$PY" -m venv "$dir" || die "venv creation failed"
  fi
  if [[ ! -f "$stamp" ]] || ! cmp -s "$req" "$stamp"; then
    log "installing $(basename "$req") into $dir"
    "$dir/bin/pip" install -q --upgrade pip >/dev/null 2>&1 || true
    "$dir/bin/pip" install -q -r "$req" || die "pip install failed for $req"
    cp "$req" "$stamp"
  fi
}

free_port() { "$PY" -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }

db_url_for() { # admin-url dbname -> url pointing at dbname
  "$PY" - "$1" "$2" <<'EOF'
import sys
from urllib.parse import urlsplit, urlunsplit
u = urlsplit(sys.argv[1])
print(urlunsplit((u.scheme, u.netloc, "/" + sys.argv[2], u.query, u.fragment)))
EOF
}

PIDS=()
DBS=()
cleanup() {
  for pid in "${PIDS[@]:-}"; do
    [[ -n "$pid" ]] && kill "$pid" 2>/dev/null && wait "$pid" 2>/dev/null
  done
  for db in "${DBS[@]:-}"; do
    [[ -n "$db" ]] && psql "$ADMIN_URL" -q -c "DROP DATABASE IF EXISTS $db WITH (FORCE)" >/dev/null 2>&1
  done
}
trap cleanup EXIT INT TERM

# start_server <label> <extra env...>; sets SERVER_URL
start_server() {
  local label="$1"; shift
  local db="qd_py_${label}_$(date +%s)_$RANDOM"
  psql "$ADMIN_URL" -q -c "CREATE DATABASE $db" >/dev/null || die "cannot create database $db via $ADMIN_URL"
  DBS+=("$db")
  local port admin
  port="$(free_port)"; admin="$(free_port)"
  while [[ "$admin" == "$port" ]]; do admin="$(free_port)"; done
  local logf="$OUT/server-$label.log"
  log "starting $label server on 127.0.0.1:$port (admin $admin, db $db, log $logf)"
  env QD_DATABASE_URL="$(db_url_for "$ADMIN_URL" "$db")" QD_BIND="127.0.0.1:$port" QD_ADMIN_BIND="127.0.0.1:$admin" \
      QD_JANITOR_INTERVAL_SECS=1 "$@" "$BIN" >"$logf" 2>&1 &
  local pid=$!
  PIDS+=("$pid")
  for _ in $(seq 1 150); do
    if curl -fsS "http://127.0.0.1:$admin/readyz" 2>/dev/null | grep -q ready; then
      SERVER_URL="http://127.0.0.1:$port"
      return 0
    fi
    kill -0 "$pid" 2>/dev/null || { tail -50 "$logf" >&2; die "$label server exited during startup"; }
    sleep 0.2
  done
  tail -50 "$logf" >&2
  die "$label server not ready after 30s"
}

# --- venvs ------------------------------------------------------------------
for p in $PROTOCOLS; do
  case "$p" in
    json) ensure_venv "$VENV_JSON" "$HERE/requirements.txt" ;;
    query) ensure_venv "$VENV_QUERY" "$HERE/requirements-legacy.txt" ;;
    *) die "unknown protocol $p" ;;
  esac
done

# --- server(s) --------------------------------------------------------------
if [[ -z "${QD_E2E_ENDPOINT:-}" && "${QD_E2E_TARGET:-}" != "aws" ]]; then
  if [[ -n "${QD_E2E_BINARY:-}" ]]; then
    BIN="$QD_E2E_BINARY"
    [[ -x "$BIN" ]] || die "QD_E2E_BINARY=$BIN is not executable"
  else
    # Always (incrementally) rebuild so the suite never runs against a stale binary.
    log "building qd"
    (cd "$ROOT" && cargo build -q -p qd) || die "cargo build failed"
    BIN="$ROOT/target/debug/qd"
  fi
  start_server main QD_PURGE_COOLDOWN_SECS=0
  export QD_E2E_ENDPOINT="$SERVER_URL" QD_E2E_PURGE_COOLDOWN_ZERO=1
  SIGV4_CREDS="AKIDE2ETEST000000001:e2e-secret-key-$RANDOM:123456789012"
  start_server sigv4 QD_AUTH_MODE=sigv4 QD_CREDENTIALS="$SIGV4_CREDS"
  export QD_E2E_SIGV4_ENDPOINT="$SERVER_URL" QD_E2E_SIGV4_CREDS="$SIGV4_CREDS"
fi

# --- tests ------------------------------------------------------------------
declare -A RESULT
rc_all=0
for p in $PROTOCOLS; do
  venv="$VENV_JSON"; [[ "$p" == query ]] && venv="$VENV_QUERY"
  log "running pytest with $p protocol ($venv)"
  (cd "$HERE" && "$venv/bin/python" -m pytest -o junit_suite_name="sqs-e2e-$p" \
      --junitxml="$OUT/junit-$p.xml" "$@")
  rc=$?
  RESULT[$p]=$rc
  [[ $rc -ne 0 ]] && rc_all=1
done

CLI_RC=skip
if [[ "${QD_E2E_CLI:-1}" == 1 ]] && command -v aws >/dev/null 2>&1; then
  log "running AWS CLI smoke test"
  "$ROOT/e2e/cli/smoke.sh"; CLI_RC=$?
  [[ $CLI_RC -ne 0 ]] && rc_all=1
fi

for p in $PROTOCOLS; do
  if [[ ${RESULT[$p]} -eq 0 ]]; then log "$p protocol: PASS"; else log "$p protocol: FAIL (pytest exit ${RESULT[$p]})"; fi
done
[[ "$CLI_RC" == skip ]] && log "aws cli smoke: skipped" || { [[ $CLI_RC -eq 0 ]] && log "aws cli smoke: PASS" || log "aws cli smoke: FAIL"; }
exit $rc_all
