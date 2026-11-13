-- One row per finished attempt of a job: who ran it, when, and how it ended.
-- A row is written in the same statement that finishes the attempt (see JobCompletionRepository),
-- so the history can never disagree with the job's state.
CREATE TABLE job_attempts (
    id           bigserial   PRIMARY KEY,
    job_id       uuid        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt      int         NOT NULL,
    worker_id    text        NOT NULL,
    started_at   timestamptz NOT NULL,
    finished_at  timestamptz,
    outcome      text,
    error        text,
    -- An attempt number exists once per job; also the lookup index for "attempts of job X".
    UNIQUE (job_id, attempt)
);
