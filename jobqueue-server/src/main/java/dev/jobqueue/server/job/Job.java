package dev.jobqueue.server.job;

import dev.jobqueue.common.JobStatus;

import java.time.Instant;
import java.util.UUID;

/**
 * A row of the {@code jobs} table. {@code payload} is kept as raw JSON text.
 */
public record Job(
        UUID id,
        String queue,
        String type,
        String payload,
        JobStatus status,
        int priority,
        Instant runAt,
        int attempts,
        int maxAttempts,
        int timeoutSeconds,
        String lockedBy,
        Instant lockedUntil,
        String idempotencyKey,
        String lastError,
        Instant createdAt,
        Instant startedAt,
        Instant finishedAt) {
}
