#!/usr/bin/env bash
# Resilience suite: rolling restarts, rolling upgrade + rollback, replica crashes, database
# outage, Postgres primary failover, migration races — all under continuous AWS SDK load.
#
# Requirements: Rust toolchain, PostgreSQL *server* binaries (initdb, pg_ctl, pg_basebackup),
# and a Postgres reachable at DATABASE_URL on which databases may be created.
#
# Environment:
#   DATABASE_URL            admin connection (default postgres://qd:qd@localhost/postgres)
#   QD_PG_BIN               directory with PostgreSQL server binaries (auto-detected)
#   QD_BINARY               binary under test (default: cargo build of this checkout)
#   QD_UPGRADE_FROM_BINARY  previous release binary for the upgrade/rollback scenario
#   QD_UPGRADE_FROM_REF     git ref to build the previous release from (default: latest tag)
#
# Extra arguments are passed to the test runner, e.g. `e2e/chaos/run.sh primary_failover`.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
log() { printf '\033[1;35m[chaos]\033[0m %s\n' "$*"; }

if [[ -z "${QD_BINARY:-}" ]]; then
  log "building qd"
  (cd "$ROOT" && cargo build -q -p qd)
  export QD_BINARY="$ROOT/target/debug/qd"
fi

if [[ -z "${QD_UPGRADE_FROM_BINARY:-}" ]]; then
  REF="${QD_UPGRADE_FROM_REF:-$(git -C "$ROOT" describe --tags --abbrev=0 2>/dev/null || true)}"
  if [[ -n "$REF" ]]; then
    SRC="$ROOT/target/upgrade-src/$REF"
    log "building previous release $REF for the upgrade scenario"
    rm -rf "$SRC"
    git -C "$ROOT" worktree add --force --detach "$SRC" "$REF" >/dev/null
    (cd "$SRC" && cargo build -q -p qd --target-dir "$ROOT/target/upgrade")
    git -C "$ROOT" worktree remove --force "$SRC" >/dev/null
    export QD_UPGRADE_FROM_BINARY="$ROOT/target/upgrade/debug/qd"
  else
    log "no previous release tag found: the upgrade scenario will upgrade this build to itself"
  fi
fi

log "binary under test: $QD_BINARY"
log "upgrade from:      ${QD_UPGRADE_FROM_BINARY:-<same build>}"
cd "$ROOT"
exec cargo test -p qd-e2e --test chaos -- --ignored --test-threads=1 --nocapture "$@"
