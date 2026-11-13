package dev.jobqueue.worker.demo;

import dev.jobqueue.common.JobContext;
import dev.jobqueue.common.JobHandler;
import dev.jobqueue.common.PermanentFailureException;
import org.springframework.stereotype.Component;

/**
 * Demo handlers that fail on purpose, to show retries and the dead-letter state.
 * Neither keeps any state: "flaky" decides from the attempt number the worker passes in.
 */
@Component
public class FailureDemoHandlers {

    public record FlakyPayload(int failTimes) {
    }

    // Boolean, not boolean: a missing field must mean "false" whatever the ObjectMapper's settings are.
    public record AlwaysFailPayload(Boolean permanent) {
    }

    /** Fails on its first {@code failTimes} attempts, then succeeds. */
    @JobHandler("flaky")
    public void flaky(JobContext context, FlakyPayload payload) {
        if (context.attempt() <= payload.failTimes()) {
            throw new IllegalStateException("flaky failure on attempt " + context.attempt());
        }
    }

    /** Fails every time; with {"permanent": true} the job goes straight to DEAD, otherwise it is retried. */
    @JobHandler("always-fail")
    public void alwaysFail(JobContext context, AlwaysFailPayload payload) {
        if (Boolean.TRUE.equals(payload.permanent())) {
            throw new PermanentFailureException("always-fail was asked to fail permanently");
        }
        throw new IllegalStateException("always-fail fails on attempt " + context.attempt());
    }
}
