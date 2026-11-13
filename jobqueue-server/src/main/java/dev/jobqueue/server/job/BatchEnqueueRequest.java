package dev.jobqueue.server.job;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;

import java.util.List;

/**
 * Body of {@code POST /jobs/batch}. The cap keeps one request (and one transaction) bounded.
 */
public record BatchEnqueueRequest(
        @NotEmpty @Size(max = 1000) List<@Valid EnqueueRequest> jobs) {
}
