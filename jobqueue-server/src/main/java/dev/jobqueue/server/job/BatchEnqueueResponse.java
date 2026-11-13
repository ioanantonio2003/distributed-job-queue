package dev.jobqueue.server.job;

import java.util.List;
import java.util.UUID;

/**
 * Ids of the created jobs, in the same order as the request.
 */
public record BatchEnqueueResponse(List<UUID> ids) {
}
