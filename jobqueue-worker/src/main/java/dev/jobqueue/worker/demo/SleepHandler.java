package dev.jobqueue.worker.demo;

import dev.jobqueue.common.JobContext;
import dev.jobqueue.common.JobHandler;
import dev.jobqueue.common.PermanentFailureException;
import org.springframework.stereotype.Component;

/**
 * Demo handler: sleeps for {@code millis}, to simulate slow work. Idempotent: sleeping twice changes nothing.
 */
@Component
public class SleepHandler {

    public record SleepPayload(long millis) {
    }

    @JobHandler("sleep")
    public void sleep(JobContext context, SleepPayload payload) throws InterruptedException {
        if (payload.millis() < 0) {
            throw new PermanentFailureException("millis must not be negative: " + payload.millis());
        }
        Thread.sleep(payload.millis());
    }
}
