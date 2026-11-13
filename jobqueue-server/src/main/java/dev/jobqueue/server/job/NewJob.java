package dev.jobqueue.server.job;

/**
 * What a caller provides to create a job; everything else (id, status, timestamps) is set by the database.
 * {@code payload} is JSON text; {@code idempotencyKey} may be null.
 */
public record NewJob(
        String queue,
        String type,
        String payload,
        int priority,
        int maxAttempts,
        int timeoutSeconds,
        String idempotencyKey) {
}
