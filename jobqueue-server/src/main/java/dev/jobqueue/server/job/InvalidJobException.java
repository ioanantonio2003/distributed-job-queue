package dev.jobqueue.server.job;

/**
 * The request is well-formed JSON but breaks a business rule (e.g. payload too large). Mapped to HTTP 400.
 */
public class InvalidJobException extends RuntimeException {

    public InvalidJobException(String message) {
        super(message);
    }
}
