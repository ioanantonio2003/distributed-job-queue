package dev.jobqueue.common;

/**
 * Thrown by a handler (or the worker) when retrying cannot help, for example an invalid payload.
 * The job goes straight to DEAD instead of being retried.
 */
public class PermanentFailureException extends RuntimeException {

    public PermanentFailureException(String message) {
        super(message);
    }

    public PermanentFailureException(String message, Throwable cause) {
        super(message, cause);
    }
}
