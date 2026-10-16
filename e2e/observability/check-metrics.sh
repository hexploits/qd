#!/usr/bin/env bash
# End-to-end check that the observability bundle matches what the server really exports.
#
#   e2e/observability/check-metrics.sh
#
# 1. starts qd in all-in-one mode (no subcommand: API + scheduler + migrations in one
#    process, so every metric of both roles is exported) on a fresh, throw-away database
#    (QD_QUEUE_METRICS_INTERVAL_SECS=2)
# 2. generates traffic with the AWS CLI v2: standard + FIFO queues, a DLQ with a redrive
#    policy, sends/batches/receives/deletes, dead-lettering, a DLQ redrive (message move task),
#    deduplication, purge, delete, message expiry, a client error and a server-side DB error
# 3. scrapes /metrics and verifies that EVERY metric referenced by
#    deploy/observability/prometheus-rules.yaml and grafana-dashboard.json exists
#    (histogram _bucket/_sum/_count suffixes allowed; `up` is exempt)
# 4. runs `promtool check metrics` on the scrape (lint findings are reported, not fatal),
#    `promtool check rules` and `promtool test rules`
#
# Environment:
#   QD_E2E_BINARY   server binary (default: `cargo build -p qd` -> target/debug)
#   DATABASE_URL    any database on the server, used to CREATE/DROP the throw-away database
#                   (default postgres://qd:qd@localhost:5432/postgres; needs CREATEDB)
#   QD_OBS_PORT / QD_OBS_ADMIN_PORT   listen ports (default 59324 / 59325)
#   PROMTOOL        promtool binary (default: promtool on PATH)
# Requires: aws (v2), psql, curl, python3, promtool.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OBS="$ROOT/deploy/observability"
PORT="${QD_OBS_PORT:-59324}"
ADMIN_PORT="${QD_OBS_ADMIN_PORT:-59325}"
PROMTOOL="${PROMTOOL:-promtool}"
BASE_URL="${DATABASE_URL:-postgres://qd:qd@localhost:5432/postgres}"
DB="qd_obs_$(date +%s)_$RANDOM"
DB_URL="$(python3 -c 'import sys, urllib.parse as u; p = u.urlsplit(sys.argv[1]); print(u.urlunsplit(p._replace(path="/" + sys.argv[2])))' "$BASE_URL" "$DB")"
WORK="$(mktemp -d)"
FAIL=0
SERVER_PID=""

log() { printf '\n== %s\n' "$*"; }
bad() { echo "FAIL: $*"; FAIL=1; }

cleanup() {
  if [[ -n "$SERVER_PID" ]]; then kill "$SERVER_PID" 2>/dev/null; wait "$SERVER_PID" 2>/dev/null; fi
  psql "$BASE_URL" -qtAc "DROP DATABASE IF EXISTS \"$DB\" WITH (FORCE)" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

for tool in aws psql curl python3 "$PROMTOOL"; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool"; exit 2; }
done

# --------------------------------------------------------------------------------------------
log "server binary"
if [[ -n "${QD_E2E_BINARY:-}" ]]; then
  BIN="$QD_E2E_BINARY"
else
  (cd "$ROOT" && cargo build -q -p qd) || { echo "cargo build failed"; exit 2; }
  BIN="$ROOT/target/debug/qd"
fi
"$BIN" --version || { echo "cannot run $BIN"; exit 2; }

log "database $DB"
psql "$BASE_URL" -qtAc "CREATE DATABASE \"$DB\"" || { echo "cannot create database"; exit 2; }

log "start server on 127.0.0.1:$PORT (admin $ADMIN_PORT)"
QD_DATABASE_URL="$DB_URL" QD_BIND="127.0.0.1:$PORT" QD_ADMIN_BIND="127.0.0.1:$ADMIN_PORT" \
  QD_QUEUE_METRICS_INTERVAL_SECS=2 QD_JANITOR_INTERVAL_SECS=2 QD_DB_STATEMENT_TIMEOUT_MS=1500 \
  QD_LOG_FORMAT=text QD_LOG="${QD_LOG:-warn}" "$BIN" >"$WORK/server.log" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 120); do
  curl -fsS "http://127.0.0.1:$ADMIN_PORT/readyz" >/dev/null 2>&1 && break
  kill -0 "$SERVER_PID" 2>/dev/null || { cat "$WORK/server.log"; echo "server exited"; exit 1; }
  sleep 0.5
done
curl -fsS "http://127.0.0.1:$ADMIN_PORT/readyz" >/dev/null || { cat "$WORK/server.log"; echo "server not ready"; exit 1; }

# --------------------------------------------------------------------------------------------
log "generate traffic"
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 AWS_PAGER="" AWS_MAX_ATTEMPTS=1
sqs() { aws --output json --endpoint-url "http://127.0.0.1:$PORT" sqs "$@"; }
jget() { python3 -c "import json,sys; d=json.load(sys.stdin); print(eval(sys.argv[1]))" "$1"; }
step() { if "$@" >/dev/null 2>"$WORK/err"; then echo "  ok: ${*:1:2}"; else echo "  error: $* -> $(cat "$WORK/err")"; FAIL=1; fi; }

DLQ=$(sqs create-queue --queue-name obs-orders-dlq | jget 'd["QueueUrl"]')
DLQ_ARN=$(sqs get-queue-attributes --queue-url "$DLQ" --attribute-names QueueArn | jget 'd["Attributes"]["QueueArn"]')
cat >"$WORK/attrs.json" <<EOF
{"VisibilityTimeout": "1",
 "RedrivePolicy": "{\"deadLetterTargetArn\":\"$DLQ_ARN\",\"maxReceiveCount\":\"1\"}"}
EOF
Q=$(sqs create-queue --queue-name obs-orders --attributes "file://$WORK/attrs.json" | jget 'd["QueueUrl"]')
FIFO=$(sqs create-queue --queue-name obs-events.fifo --attributes FifoQueue=true | jget 'd["QueueUrl"]')
EXP=$(sqs create-queue --queue-name obs-expiring --attributes MessageRetentionPeriod=60 | jget 'd["QueueUrl"]')
TMP=$(sqs create-queue --queue-name obs-temp | jget 'd["QueueUrl"]')
[[ -n "$DLQ" && -n "$Q" && -n "$FIFO" && -n "$EXP" && -n "$TMP" ]] || { bad "create-queue"; exit 1; }
echo "  queues: $Q $DLQ $FIFO $EXP $TMP"
EXPIRY_START=$(date +%s)
step sqs send-message --queue-url "$EXP" --message-body "expires in 60s"

step sqs send-message --queue-url "$Q" --message-body "hello"
step sqs send-message-batch --queue-url "$Q" --entries Id=a,MessageBody=one Id=b,MessageBody=two
# receive + delete one message
RH=$(sqs receive-message --queue-url "$Q" --max-number-of-messages 1 | jget 'd["Messages"][0]["ReceiptHandle"]')
step sqs delete-message --queue-url "$Q" --receipt-handle "$RH"
# receive the rest once, let the 1s visibility timeout lapse, receive again -> dead-lettered
step sqs receive-message --queue-url "$Q" --max-number-of-messages 10
sleep 2
step sqs receive-message --queue-url "$Q" --max-number-of-messages 10 --wait-time-seconds 1
# empty receive (short + long poll)
step sqs receive-message --queue-url "$TMP" --wait-time-seconds 1
# FIFO deduplication
step sqs send-message --queue-url "$FIFO" --message-body x --message-group-id g --message-deduplication-id d1
step sqs send-message --queue-url "$FIFO" --message-body x --message-group-id g --message-deduplication-id d1
# purge + delete
step sqs send-message --queue-url "$TMP" --message-body purge-me
step sqs purge-queue --queue-url "$TMP"
# client error (4xx): non-existent queue
if sqs get-queue-url --queue-name obs-does-not-exist >/dev/null 2>&1; then bad "expected NonExistentQueue"; else echo "  ok: client error"; fi
# wait for the DLQ gauges, then redrive the DLQ back to its source (message move task)
sleep 3
step sqs start-message-move-task --source-arn "$DLQ_ARN"
step sqs delete-queue --queue-url "$TMP"
sleep 2  # let the move task finish before the lock below
# server-side DB error: hold an exclusive lock on the messages table longer than the
# 1.5s statement timeout while sending -> InternalFailure + qd_db_errors_total{kind="other"}
psql "$DB_URL" -qtAc "BEGIN; LOCK TABLE qd_messages IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(4); COMMIT;" >/dev/null 2>&1 &
LOCK_PID=$!
sleep 1
if sqs send-message --queue-url "$Q" --message-body during-lock >/dev/null 2>&1; then
  echo "  warning: send during table lock succeeded (no DB error generated)"
else
  echo "  ok: server error during table lock"
fi
wait "$LOCK_PID"

# message expiry needs the 60s retention period to elapse plus a janitor pass
scrape() { curl -fsS "http://127.0.0.1:$ADMIN_PORT/metrics" >"$WORK/metrics.txt"; }
log "waiting for message expiry (retention 60s)"
for _ in $(seq 1 60); do
  scrape && grep -q '^qd_messages_expired_total' "$WORK/metrics.txt" && break
  sleep 2
done
echo "  expired after $(( $(date +%s) - EXPIRY_START ))s"
sleep 3  # one more queue-gauge refresh
scrape || { bad "scrape failed"; exit 1; }
cp "$WORK/metrics.txt" "${QD_OBS_SCRAPE_OUT:-/dev/null}" 2>/dev/null || true
echo "  scraped $(grep -vc '^#' "$WORK/metrics.txt") samples"

# --------------------------------------------------------------------------------------------
log "metrics referenced by rules + dashboard exist in the scrape"
python3 - "$OBS/prometheus-rules.yaml" "$OBS/grafana-dashboard.json" "$WORK/metrics.txt" <<'PY' || FAIL=1
import json, re, sys
rules_path, dash_path, scrape_path = sys.argv[1:4]

def rule_exprs(path):
    lines = open(path).read().split("\n")
    i = 0
    while i < len(lines):
        m = re.match(r"^(\s*)(?:- )?expr:\s*(.*)$", lines[i])
        if m:
            indent, rest = len(m.group(1)), m.group(2)
            if rest in ("|", ">", "|-", ">-"):
                block = []
                i += 1
                while i < len(lines) and (not lines[i].strip() or len(lines[i]) - len(lines[i].lstrip()) > indent):
                    block.append(lines[i]); i += 1
                yield "\n".join(block)
                continue
            yield rest
        i += 1

def dash_exprs(path):
    d = json.load(open(path))
    def walk(panels):
        for p in panels:
            for t in p.get("targets", []):
                yield t.get("expr", "")
            yield from walk(p.get("panels", []))
    yield from walk(d["panels"])
    for v in d["templating"]["list"]:
        q = v.get("query")
        yield q.get("query", "") if isinstance(q, dict) else (q if v.get("type") == "query" else "")

FUNCS = {"sum", "max", "min", "avg", "count", "rate", "increase", "deriv", "histogram_quantile", "by",
         "without", "on", "ignoring", "group_left", "group_right", "and", "or", "unless", "label_values",
         "bool", "offset", "irate", "delta", "abs", "vector", "scalar", "topk", "bottomk", "time", "le"}
def metrics_in(expr):
    expr = re.sub(r"label_values\((.*),\s*\w+\s*\)", r"\1", expr)  # label_values(<selector>, <label>)
    expr = re.sub(r'"(?:[^"\\]|\\.)*"', '""', expr)          # drop string literals
    expr = re.sub(r"\{[^}]*\}", "{}", expr)                   # drop label matchers
    expr = re.sub(r"\[[^\]]*\]", "[]", expr)                  # drop ranges
    expr = re.sub(r"\b(by|without|on|ignoring|group_left|group_right)\s*\([^)]*\)", " ", expr)
    out = set()
    for tok in re.findall(r"(?<![\w:$])([a-zA-Z_][a-zA-Z0-9_:]*)", expr):
        if ":" in tok or tok in FUNCS or tok.startswith("__"):
            continue                                          # recording rules, functions
        out.add(tok)
    return out

referenced = {}
for src, exprs in (("rules", rule_exprs(rules_path)), ("dashboard", dash_exprs(dash_path))):
    for e in exprs:
        for m in metrics_in(e):
            referenced.setdefault(m, set()).add(src)

scraped = set()
for line in open(scrape_path):
    if line.startswith("#") or not line.strip():
        continue
    scraped.add(re.split(r"[{\s]", line, 1)[0])

missing = []
for m in sorted(referenced):
    if m == "up":
        continue
    ok = m in scraped
    if not ok:
        for suf in ("_bucket", "_sum", "_count"):
            if m.endswith(suf) and (m in scraped or m[: -len(suf)] + "_bucket" in scraped):
                ok = True
    print(f"  {'ok     ' if ok else 'MISSING'} {m}  ({', '.join(sorted(referenced[m]))})")
    if not ok:
        missing.append(m)
print(f"{len(referenced)} referenced metric names, {len(missing)} missing")
sys.exit(1 if missing else 0)
PY

log "sanity of a few values"
python3 - "$WORK/metrics.txt" <<'PY' || FAIL=1
import re, sys
samples = {}
for line in open(sys.argv[1]):
    if line.startswith("#") or not line.strip():
        continue
    key, val = line.rsplit(" ", 1) if "}" not in line else (line[: line.rindex("}") + 1], line[line.rindex("}") + 1 :])
    samples[key.strip()] = float(val.split()[0])
def find(name, **labels):
    for k, v in samples.items():
        if k.split("{")[0] == name and all(f'{l}="{x}"' in k for l, x in labels.items()):
            yield k, v
checks = [
    ("qd_ready == 1", any(v == 1 for _, v in find("qd_ready"))),
    ("qd_notifier_connected == 1", any(v == 1 for _, v in find("qd_notifier_connected"))),
    ("DLQ flagged by qd_queue_info{is_dlq=true}", any(True for _ in find("qd_queue_info", queue="obs-orders-dlq", is_dlq="true"))),
    ("FIFO queue type", any(True for _ in find("qd_queue_info", queue="obs-events.fifo", type="fifo"))),
    ("dead-lettered counted on source queue", any(v >= 1 for _, v in find("qd_messages_dead_lettered_total", queue="obs-orders"))),
    ("deduplicated >= 1", any(v >= 1 for _, v in find("qd_messages_deduplicated_total"))),
    ("client error recorded", any(True for _ in find("qd_requests_total", code="AWS.SimpleQueueService.NonExistentQueue"))),
    ('qd_build_info{role="all"}', any(True for _ in find("qd_build_info", role="all"))),
    ("scheduler heartbeat fresh (< 60s)", any(v < 60 for _, v in find("qd_scheduler_heartbeat_age_seconds"))),
    ("scheduler heartbeats written", any(v >= 1 for _, v in find("qd_scheduler_heartbeats_total"))),
]
bad = 0
for name, ok in checks:
    print(f"  {'ok  ' if ok else 'FAIL'} {name}")
    bad += not ok
sys.exit(1 if bad else 0)
PY

log "promtool check metrics (lint; findings are reported, not fatal)"
if "$PROMTOOL" check metrics <"$WORK/metrics.txt" >"$WORK/lint.txt" 2>&1; then
  echo "  clean"
else
  sed 's/^/  /' "$WORK/lint.txt"
  echo "  ($(wc -l <"$WORK/lint.txt") lint finding(s), tolerated)"
fi

log "promtool check rules"
"$PROMTOOL" check rules "$OBS/prometheus-rules.yaml" || FAIL=1
log "promtool test rules"
"$PROMTOOL" test rules "$OBS"/tests/*.test.yaml || FAIL=1

if [[ $FAIL -ne 0 ]]; then
  log "server log (tail)"
  tail -n 40 "$WORK/server.log"
  echo; echo "check-metrics: FAILED"
  exit 1
fi
echo; echo "check-metrics: OK"
