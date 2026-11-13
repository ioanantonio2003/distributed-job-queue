package dev.jobqueue.worker.handler;

/**
 * No handler is registered in this worker for the job's type.
 */
public class UnknownJobTypeException extends RuntimeException {

    public UnknownJobTypeException(String type) {
        super("No handler registered for job type '" + type + "'");
    }
}
