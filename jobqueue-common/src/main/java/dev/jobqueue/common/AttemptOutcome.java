package dev.jobqueue.common;

/**
 * How one attempt of a job ended; stored in {@code job_attempts.outcome}.
 */
public enum AttemptOutcome {
    SUCCEEDED,
    FAILED,
    TIMED_OUT
}
