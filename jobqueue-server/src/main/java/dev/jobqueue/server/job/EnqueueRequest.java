package dev.jobqueue.server.job;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import tools.jackson.databind.JsonNode;

import java.time.Instant;

/**
 * Body of {@code POST /jobs}. Optional numbers are boxed so "absent" can be told apart from 0,
 * and the defaults are applied in the constructor, before validation runs.
 *
 * <p>{@code priority}: higher runs first, from -100 to 100. {@code delaySeconds}: wait this long, measured by the
 * database clock. {@code runAt}: start not before this absolute time (ISO-8601); a time in the past means
 * "now". Give at most one of {@code delaySeconds} and {@code runAt}.
 */
public record EnqueueRequest(
        @NotBlank @Size(max = 100) String queue,
        @NotBlank @Size(max = 100) String type,
        JsonNode payload,
        @Min(-100) @Max(100) Integer priority,
        @Min(1) @Max(50) Integer maxAttempts,
        @Min(1) Integer timeoutSeconds,
        @Size(min = 1, max = 200) String idempotencyKey,
        @Min(0) @Max(EnqueueRequest.MAX_DELAY_SECONDS) Integer delaySeconds,
        Instant runAt) {

    /** 30 days: a job scheduled further ahead is almost certainly a mistake (for example seconds vs milliseconds). */
    static final int MAX_DELAY_SECONDS = 30 * 24 * 60 * 60;

    public EnqueueRequest {
        priority = priority == null ? 0 : priority;
        maxAttempts = maxAttempts == null ? 5 : maxAttempts;
        timeoutSeconds = timeoutSeconds == null ? 60 : timeoutSeconds;
    }
}
