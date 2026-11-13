package dev.jobqueue.server.job;

import java.util.UUID;

/**
 * No job has the requested id. Mapped to HTTP 404.
 */
public class JobNotFoundException extends RuntimeException {

    public JobNotFoundException(UUID id) {
        super("Job " + id + " not found");
    }
}
