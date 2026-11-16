package dev.jobqueue.server.job;

import java.time.Instant;

/**
 * One row of a job's history (the job_attempts table). {@code finishedAt} and {@code outcome} are always
 * set today; {@code outcome} is SUCCEEDED, FAILED, TIMED_OUT or LEASE_EXPIRED.
 */
public record AttemptResponse(
        int attempt,
        String workerId,
        Instant startedAt,
        Instant finishedAt,
        String outcome,
        String error) {
}
