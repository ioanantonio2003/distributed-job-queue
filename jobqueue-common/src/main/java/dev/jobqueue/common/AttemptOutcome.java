package dev.jobqueue.common;

/**
 * How one attempt of a job ended; stored in {@code job_attempts.outcome}.
 */
public enum AttemptOutcome {
    SUCCEEDED,
    FAILED,
    TIMED_OUT,
    /** The worker stopped heartbeating (it died or was cut off) and the reaper took the job back. */
    LEASE_EXPIRED
}
