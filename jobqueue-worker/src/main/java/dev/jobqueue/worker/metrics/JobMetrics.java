package dev.jobqueue.worker.metrics;

import dev.jobqueue.worker.claim.ClaimedJob;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * The worker's metrics, in one place so the poller code stays readable. Prometheus reads them from
 * /actuator/prometheus; the names below become jobqueue_..._total, jobqueue_..._seconds_bucket and so on.
 *
 * <p>Tags are kept few and bounded (queue, job type, a handful of outcomes): every distinct tag combination
 * is a separate time series, so something like a job id must never be a tag.
 */
@Component
public class JobMetrics {

    private final MeterRegistry registry;

    public JobMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    /** A job was just claimed: record how long it waited since it became runnable. */
    public void claimed(ClaimedJob job) {
        Timer.builder("jobqueue.job.wait")
                .description("Time a runnable job waited in its queue before a worker started it")
                .tag("queue", job.queue())
                .publishPercentileHistogram()
                .maximumExpectedValue(Duration.ofMinutes(10))
                .register(registry)
                .record(Duration.ofNanos((long) (Math.max(0, job.waitedSeconds()) * 1_000_000_000L)));
    }

    /** A handler returned or threw: how long it ran, and whether it succeeded. */
    public void handlerFinished(ClaimedJob job, boolean succeeded, Duration took) {
        Timer.builder("jobqueue.handler.duration")
                .description("How long job handlers run")
                .tag("queue", job.queue())
                .tag("type", job.type())
                .tag("outcome", succeeded ? "success" : "error")
                .publishPercentileHistogram()
                .maximumExpectedValue(Duration.ofMinutes(5))
                .register(registry)
                .record(took);
    }

    /**
     * What became of an attempt: succeeded, failed (the handler threw), timed_out, or lease_lost (another worker
     * owns the job now and this result was ignored).
     */
    public void processed(String queue, String outcome) {
        Counter.builder("jobqueue.jobs.processed")
                .description("Attempts finished by this worker, by outcome")
                .tag("queue", queue)
                .tag("outcome", outcome)
                .register(registry)
                .increment();
    }

    /** A failed attempt used up the job's last attempt (or was permanent): the job is now DEAD. */
    public void dead(String queue) {
        Counter.builder("jobqueue.jobs.dead")
                .description("Jobs that ended up DEAD")
                .tag("queue", queue)
                .register(registry)
                .increment();
    }
}
