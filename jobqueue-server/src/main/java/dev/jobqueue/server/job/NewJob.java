package dev.jobqueue.server.job;

import java.time.Instant;

/**
 * What a caller provides to create a job; everything else (id, status, created_at) is set by the database.
 * {@code payload} is JSON text; {@code idempotencyKey} may be null.
 *
 * <p>When the job may start: {@code runAt} (an absolute time) if given, otherwise {@code delaySeconds} after
 * the database's current time. At most one of them is meaningful; callers pass delay 0 and no runAt for "now".
 */
public record NewJob(
        String queue,
        String type,
        String payload,
        int priority,
        int maxAttempts,
        int timeoutSeconds,
        String idempotencyKey,
        int delaySeconds,
        Instant runAt) {

    /** A job that may start immediately. */
    public NewJob(String queue, String type, String payload, int priority, int maxAttempts, int timeoutSeconds,
            String idempotencyKey) {
        this(queue, type, payload, priority, maxAttempts, timeoutSeconds, idempotencyKey, 0, null);
    }
}
