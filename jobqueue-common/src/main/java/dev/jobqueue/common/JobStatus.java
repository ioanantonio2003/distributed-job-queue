package dev.jobqueue.common;

/**
 * Lifecycle of a job. The names must match the values allowed by the {@code jobs_status_chk} constraint.
 */
public enum JobStatus {
    QUEUED,
    RUNNING,
    SUCCEEDED,
    DEAD,
    CANCELLED
}
