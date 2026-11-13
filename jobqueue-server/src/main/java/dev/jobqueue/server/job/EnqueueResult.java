package dev.jobqueue.server.job;

/**
 * Outcome of a single enqueue: {@code created} is false when an idempotency key matched an existing job.
 */
public record EnqueueResult(JobResponse job, boolean created) {
}
