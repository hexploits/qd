-- qd initial schema.
--
-- All time arithmetic happens in the database (now()) so that replicas with skewed clocks agree
-- on visibility and retention.

CREATE TABLE qd_queues (
    id              BIGSERIAL PRIMARY KEY,
    account_id      TEXT        NOT NULL,
    name            TEXT        NOT NULL,
    fifo            BOOLEAN     NOT NULL,
    attributes      JSONB       NOT NULL,
    tags            JSONB       NOT NULL DEFAULT '{}'::jsonb,
    -- Denormalised from attributes.redrive_policy so ListDeadLetterSourceQueues is an index scan.
    dlq_target_arn  TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_purged_at  TIMESTAMPTZ,
    UNIQUE (account_id, name)
);

CREATE INDEX qd_queues_dlq_target_idx ON qd_queues (dlq_target_arn) WHERE dlq_target_arn IS NOT NULL;

-- Queues that were deleted and whose messages still need to be reclaimed by the janitor.
CREATE TABLE qd_deleted_queues (
    queue_id    BIGINT      PRIMARY KEY,
    deleted_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE qd_messages (
    id                  BIGSERIAL   PRIMARY KEY,   -- doubles as the FIFO sequence number
    queue_id            BIGINT      NOT NULL,
    message_id          UUID        NOT NULL,
    body                TEXT        NOT NULL,
    message_attributes  JSONB,
    system_attributes   JSONB,
    sender_id           TEXT        NOT NULL,
    sent_at             TIMESTAMPTZ NOT NULL,
    visible_at          TIMESTAMPTZ NOT NULL,
    expires_at          TIMESTAMPTZ NOT NULL,
    receive_count       INTEGER     NOT NULL DEFAULT 0,
    first_received_at   TIMESTAMPTZ,
    receipt_token       UUID,
    group_id            TEXT,
    dedup_id            TEXT,
    dlq_source_arn      TEXT
);

CREATE INDEX qd_messages_visible_idx ON qd_messages (queue_id, visible_at, id);
CREATE INDEX qd_messages_group_idx ON qd_messages (queue_id, group_id, id) WHERE group_id IS NOT NULL;
CREATE INDEX qd_messages_expires_idx ON qd_messages (expires_at);

-- Messages churn constantly; vacuum aggressively to keep the visibility index tight.
ALTER TABLE qd_messages SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_vacuum_cost_delay = 1
);

-- One row per FIFO message group that has (or recently had) messages.
CREATE TABLE qd_fifo_groups (
    queue_id      BIGINT      NOT NULL,
    group_id      TEXT        NOT NULL,
    last_send_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (queue_id, group_id)
);

-- FIFO deduplication window (5 minutes).
CREATE TABLE qd_fifo_dedup (
    queue_id         BIGINT      NOT NULL,
    scope            TEXT        NOT NULL,  -- '' for queue scope, the group id for messageGroup scope
    dedup_id         TEXT        NOT NULL,
    message_id       UUID        NOT NULL,
    sequence_number  BIGINT      NOT NULL,
    expires_at       TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (queue_id, scope, dedup_id)
);

CREATE INDEX qd_fifo_dedup_expires_idx ON qd_fifo_dedup (expires_at);

-- FIFO ReceiveRequestAttemptId deduplication (5 minutes).
CREATE TABLE qd_receive_attempts (
    queue_id    BIGINT      NOT NULL,
    attempt_id  TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    entries     JSONB       NOT NULL,
    PRIMARY KEY (queue_id, attempt_id)
);

CREATE INDEX qd_receive_attempts_created_idx ON qd_receive_attempts (created_at);

-- Dead-letter queue redrive tasks (StartMessageMoveTask).
CREATE TABLE qd_move_tasks (
    id                 UUID        PRIMARY KEY,
    source_queue_id    BIGINT      NOT NULL,
    source_arn         TEXT        NOT NULL,
    destination_arn    TEXT,
    max_per_second     INTEGER,
    status             TEXT        NOT NULL,   -- RUNNING | COMPLETED | CANCELLING | CANCELLED | FAILED
    moved              BIGINT      NOT NULL DEFAULT 0,
    to_move            BIGINT      NOT NULL DEFAULT 0,
    failure_reason     TEXT,
    cutoff_id          BIGINT      NOT NULL,   -- only messages that arrived before the task (id <= cutoff) move
    started_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX qd_move_tasks_source_idx ON qd_move_tasks (source_queue_id, started_at DESC);
CREATE INDEX qd_move_tasks_running_idx ON qd_move_tasks (status) WHERE status IN ('RUNNING', 'CANCELLING');
-- At most one active move task per source queue.
CREATE UNIQUE INDEX qd_move_tasks_one_active_idx ON qd_move_tasks (source_queue_id) WHERE status IN ('RUNNING', 'CANCELLING');
