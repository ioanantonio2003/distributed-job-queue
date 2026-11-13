package dev.jobqueue.worker.claim;

import java.util.UUID;

/**
 * A job this worker has just leased. {@code attempt} is the value of {@code attempts} right after the claim:
 * together with the worker id it is the fencing token that every later update must present.
 */
public record ClaimedJob(
        UUID id,
        String queue,
        String type,
        String payload,
        int attempt,
        int maxAttempts,
        int timeoutSeconds,
        String idempotencyKey) {
}
