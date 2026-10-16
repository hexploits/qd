-- Liveness of scheduler processes (background maintenance). Server pods use the freshest
-- heartbeat to report scheduler health and to (optionally) wait for a scheduler at startup.
-- Additive only: older releases ignore this table.
CREATE TABLE qd_scheduler_heartbeats (
    instance    TEXT        PRIMARY KEY,
    version     TEXT        NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen   TIMESTAMPTZ NOT NULL DEFAULT now()
);
