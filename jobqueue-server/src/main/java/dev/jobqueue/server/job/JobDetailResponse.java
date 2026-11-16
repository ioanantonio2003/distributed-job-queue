package dev.jobqueue.server.job;

import com.fasterxml.jackson.annotation.JsonUnwrapped;

import java.util.List;

/**
 * A job with its history. The job's own fields stay at the top level of the JSON (unwrapped), so existing
 * clients of GET /jobs/{id} keep working; {@code history} is added next to them.
 */
public record JobDetailResponse(
        @JsonUnwrapped JobResponse job,
        List<AttemptResponse> history) {
}
