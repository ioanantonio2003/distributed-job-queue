package dev.jobqueue.common;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method of a Spring bean as the handler for jobs of one type.
 *
 * <p>The method must be {@code void} and take exactly two parameters: a {@link JobContext}, then the
 * payload. The payload parameter can be any type Jackson can build from the job's JSON
 * (a record, a {@code Map}, a {@code JsonNode}, ...). The method must be idempotent: delivery is at-least-once,
 * so it can run more than once for the same job.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface JobHandler {

    /** The job type this method handles; must be unique across all handlers. */
    String value();
}
