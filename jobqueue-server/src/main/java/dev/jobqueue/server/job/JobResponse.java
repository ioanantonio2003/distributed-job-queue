package dev.jobqueue.server.job;

import dev.jobqueue.common.JobStatus;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

/**
 * What the API returns for a job. Lease internals (locked_by, locked_until) are deliberately not exposed.
 */
public record JobResponse(
        UUID id,
        String queue,
        String type,
        JsonNode payload,
        JobStatus status,
        int priority,
        Instant runAt,
        int attempts,
        int maxAttempts,
        int timeoutSeconds,
        String lastError,
        Instant createdAt,
        Instant startedAt,
        Instant finishedAt) {

    static JobResponse from(Job job, JsonNode payload) {
        return new JobResponse(job.id(), job.queue(), job.type(), payload, job.status(), job.priority(),
                job.runAt(), job.attempts(), job.maxAttempts(), job.timeoutSeconds(), job.lastError(),
                job.createdAt(), job.startedAt(), job.finishedAt());
    }
}
