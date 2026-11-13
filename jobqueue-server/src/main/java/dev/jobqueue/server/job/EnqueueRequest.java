package dev.jobqueue.server.job;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import tools.jackson.databind.JsonNode;

/**
 * Body of {@code POST /jobs}. Optional numbers are boxed so "absent" can be told apart from 0,
 * and the defaults are applied in the constructor, before validation runs.
 */
public record EnqueueRequest(
        @NotBlank @Size(max = 100) String queue,
        @NotBlank @Size(max = 100) String type,
        JsonNode payload,
        Integer priority,
        @Min(1) @Max(50) Integer maxAttempts,
        @Min(1) Integer timeoutSeconds) {

    public EnqueueRequest {
        priority = priority == null ? 0 : priority;
        maxAttempts = maxAttempts == null ? 5 : maxAttempts;
        timeoutSeconds = timeoutSeconds == null ? 60 : timeoutSeconds;
    }
}
