# End-to-end tests (black box)

These tests treat qd as an opaque SQS endpoint and drive it with the **real AWS
clients**, so they double as a parity check against real AWS SQS.

| Path | What |
| --- | --- |
| `python/` | pytest suite using boto3, run under **both** SQS wire protocols |
| `python/run.sh` | one-shot runner: venvs, throwaway DB, servers, pytest x2, CLI smoke |
| `cli/smoke.sh` | AWS CLI v2 smoke script (create/send/receive/delete/attributes/FIFO/DLQ) |

## Two protocols

| venv | SDK | Wire protocol |
| --- | --- | --- |
| `.venv-json` (`requirements.txt`) | latest boto3 | AWS JSON 1.0 (`X-Amz-Target: AmazonSQS.*`) |
| `.venv-query` (`requirements-legacy.txt`) | boto3 1.28.80 / botocore 1.31.80 | legacy Query (form-encoded, XML) |

botocore 1.31.80 is the last release whose SQS model uses the Query protocol (1.31.81 switched to
JSON). Every test id is suffixed with the protocol, e.g. `test_fifo.py::test_fifo_group_blocking[query]`,
and JUnit reports are written to `python/.out/junit-{json,query}.xml`.

## Running against qd

Requires Postgres (a role that can `CREATE DATABASE`), `psql`, `curl`, Python 3.10+, cargo.

```sh
e2e/python/run.sh                 # everything
e2e/python/run.sh -k fifo -x      # extra args go to pytest
QD_E2E_PROTOCOLS=json e2e/python/run.sh
```

`run.sh` creates/reuses the venvs, builds `target/debug/qd` if missing (or uses
`QD_E2E_BINARY`), creates a fresh database via `DATABASE_URL` (default
`postgres://qd:qd@localhost/postgres`), and starts two servers on free ports:

* the main server (`QD_AUTH_MODE=none`, `QD_PURGE_COOLDOWN_SECS=0`, `QD_JANITOR_INTERVAL_SECS=1`);
* a SigV4 server (`QD_AUTH_MODE=sigv4`, default 60s purge cooldown) used by `test_auth.py`.

It runs pytest once per protocol, then `cli/smoke.sh` (if `aws` is on PATH; `QD_E2E_CLI=0`
disables it), stops the servers, drops the databases, and exits non-zero if anything failed.
Server logs are in `python/.out/server-*.log`.

To test an already-running server: `QD_E2E_ENDPOINT=http://127.0.0.1:9324 e2e/python/run.sh`
(no server is started; SigV4 tests are skipped). Running pytest directly also works:
`cd e2e/python && QD_E2E_ENDPOINT=... .venv-json/bin/pytest`.

## Running against real AWS SQS

```sh
AWS_PROFILE=sandbox AWS_REGION=us-east-1 QD_E2E_TARGET=aws e2e/python/run.sh
QD_E2E_TARGET=aws e2e/cli/smoke.sh
```

Uses the default credential chain. Queues are named `e2e-<run>-...` and deleted on teardown.
Tests marked `qd_only` are skipped: they rely on qd knobs (zero purge cooldown, a fast
move-task worker), unsigned raw HTTP requests, or the SigV4 test server. Note that AWS
attribute counters (`ApproximateNumberOf*`) are eventually consistent, so a few count
assertions may be flaky against AWS.

## Markers

* `qd_only`: skipped when `QD_E2E_TARGET=aws`
* `json_only` / `query_only`: skipped under the other protocol

Known server bugs are kept as `xfail(strict=True, reason="server bug: ...")`, so the suite
stays green and flags the moment a bug is fixed (the xfail then turns into a failure, and the
marker should be removed).
