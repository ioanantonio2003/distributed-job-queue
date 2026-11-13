package dev.jobqueue.common;

import java.util.UUID;

/**
 * Facts about the job being executed, passed to every handler.
 * {@code attempt} starts at 1; {@code idempotencyKey} is null when the job was enqueued without one.
 */
public record JobContext(UUID jobId, String queue, String type, int attempt, String idempotencyKey) {
}
