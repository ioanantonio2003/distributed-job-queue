package dev.jobqueue.worker.demo;

import dev.jobqueue.common.JobContext;
import dev.jobqueue.common.JobHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

import java.time.Duration;

/**
 * Demo handlers for two kinds of work with very different speeds, to show queue isolation:
 * enqueue "send-email" jobs in an {@code emails} queue (fast) and "generate-report" jobs in a {@code reports}
 * queue (slow), and give the queues their own concurrency, e.g. {@code JOBQUEUE_WORKER_CONCURRENCY_EMAILS=4}
 * and {@code JOBQUEUE_WORKER_CONCURRENCY_REPORTS=2}. Both are idempotent: they only log or wait.
 */
@Component
public class QueueDemoHandlers {

    private static final Logger log = LoggerFactory.getLogger(QueueDemoHandlers.class);

    public record ReportPayload(Long millis) {
    }

    /** Fast: returns at once. */
    @JobHandler("send-email")
    public void sendEmail(JobContext context, JsonNode payload) {
        log.info("send-email job {} (attempt {}): {}", context.jobId(), context.attempt(), payload);
    }

    /** Slow: takes {@code millis} (3 s when not given), like a report that needs time. */
    @JobHandler("generate-report")
    public void generateReport(JobContext context, ReportPayload payload) throws InterruptedException {
        long millis = payload.millis() == null ? 3_000 : payload.millis();
        Thread.sleep(Duration.ofMillis(millis));
        log.info("generate-report job {} done after {} ms", context.jobId(), millis);
    }
}
