package dev.jobqueue.worker.demo;

import dev.jobqueue.common.JobContext;
import dev.jobqueue.common.JobHandler;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

/**
 * Does nothing, on purpose: no logging, no work. The benchmark uses it so that what is measured is the queue
 * itself (claim, heartbeat, completion), not a handler. Trivially idempotent.
 */
@Component
public class NoopHandler {

    @JobHandler("noop")
    public void noop(JobContext context, JsonNode payload) {
        // intentionally empty
    }
}
