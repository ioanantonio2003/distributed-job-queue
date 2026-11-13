CREATE TABLE jobs (
    id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    queue            text        NOT NULL,
    type             text        NOT NULL,
    payload          jsonb       NOT NULL DEFAULT '{}',
    status           text        NOT NULL DEFAULT 'QUEUED',
    priority         int         NOT NULL DEFAULT 0,
    run_at           timestamptz NOT NULL DEFAULT now(),
    attempts         int         NOT NULL DEFAULT 0,
    max_attempts     int         NOT NULL DEFAULT 5,
    timeout_seconds  int         NOT NULL DEFAULT 60,
    locked_by        text,
    locked_until     timestamptz,
    idempotency_key  text,
    last_error       text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    started_at       timestamptz,
    finished_at      timestamptz,
    CONSTRAINT jobs_status_chk
        CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'DEAD', 'CANCELLED'))
);

-- Partial index: contains only QUEUED rows, so it stays small however many finished jobs pile up.
-- Column order matches the claim query: filter by queue, order by priority then run_at.
CREATE INDEX jobs_claim_idx ON jobs (queue, priority DESC, run_at) WHERE status = 'QUEUED';

-- Partial index: only RUNNING rows, used by the reaper to find expired leases.
CREATE INDEX jobs_lease_idx ON jobs (locked_until) WHERE status = 'RUNNING';

-- Idempotent enqueue: the same key in the same queue can exist only once; NULL keys are not constrained.
CREATE UNIQUE INDEX jobs_idem_idx ON jobs (queue, idempotency_key) WHERE idempotency_key IS NOT NULL;
