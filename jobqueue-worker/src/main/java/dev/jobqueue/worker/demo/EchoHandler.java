package dev.jobqueue.worker.demo;

import dev.jobqueue.common.JobContext;
import dev.jobqueue.common.JobHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

/**
 * Demo handler: logs the payload. Idempotent, since logging twice does no harm.
 */
@Component
public class EchoHandler {

    private static final Logger log = LoggerFactory.getLogger(EchoHandler.class);

    @JobHandler("echo")
    public void echo(JobContext context, JsonNode payload) {
        log.info("echo job {} (attempt {}): {}", context.jobId(), context.attempt(), payload);
    }
}
