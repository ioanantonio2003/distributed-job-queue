package dev.jobqueue.server.job;

/**
 * The job exists but is not in a state that allows the requested change (e.g. cancelling a RUNNING job).
 * Mapped to HTTP 409.
 */
public class InvalidJobStateException extends RuntimeException {

    public InvalidJobStateException(String message) {
        super(message);
    }
}
