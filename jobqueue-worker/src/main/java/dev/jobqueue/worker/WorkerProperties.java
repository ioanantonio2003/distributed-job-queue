package dev.jobqueue.worker;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.net.InetAddress;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Worker settings, bound from {@code jobqueue.worker.*} (or env vars such as JOBQUEUE_WORKER_SLOTS).
 * The defaults are applied by Spring when binding; direct construction (tests) must pass every value.
 *
 * @param workerId        unique id written to jobs.locked_by; generated (hostname + random suffix) when absent
 * @param queues          queues this worker takes jobs from
 * @param slots           maximum number of handlers running at the same time
 * @param lease           how long a claimed job stays reserved for this worker
 * @param pollInterval    first wait after an empty poll
 * @param maxPollInterval upper bound of the wait; it doubles from pollInterval while polls stay empty
 * @param retryBaseDelay  wait before the first retry of a failed job (doubles with each further attempt)
 * @param retryMaxDelay   upper bound of the wait between retries
 * @param heartbeatInterval how often the lease of running jobs is extended; a third of the lease when absent
 */
@ConfigurationProperties("jobqueue.worker")
public record WorkerProperties(
        String workerId,
        @DefaultValue("default") List<String> queues,
        @DefaultValue("4") int slots,
        @DefaultValue("30s") Duration lease,
        @DefaultValue("500ms") Duration pollInterval,
        @DefaultValue("5s") Duration maxPollInterval,
        @DefaultValue("2s") Duration retryBaseDelay,
        @DefaultValue("5m") Duration retryMaxDelay,
        Duration heartbeatInterval) {

    public WorkerProperties {
        if (workerId == null || workerId.isBlank()) {
            workerId = defaultWorkerId();
        }
        if (queues.isEmpty()) {
            throw new IllegalArgumentException("jobqueue.worker.queues must not be empty");
        }
        if (slots < 1) {
            throw new IllegalArgumentException("jobqueue.worker.slots must be at least 1");
        }
        if (lease.isNegative() || lease.isZero() || pollInterval.isNegative() || pollInterval.isZero()) {
            throw new IllegalArgumentException("jobqueue.worker.lease and poll-interval must be positive");
        }
        if (maxPollInterval.compareTo(pollInterval) < 0) {
            throw new IllegalArgumentException("jobqueue.worker.max-poll-interval must be >= poll-interval");
        }
        if (retryBaseDelay.isNegative() || retryBaseDelay.isZero() || retryMaxDelay.compareTo(retryBaseDelay) < 0) {
            throw new IllegalArgumentException(
                    "jobqueue.worker.retry-base-delay must be positive and <= retry-max-delay");
        }
        if (heartbeatInterval == null) {
            heartbeatInterval = lease.dividedBy(3);
        }
        if (heartbeatInterval.isNegative() || heartbeatInterval.isZero()) {
            throw new IllegalArgumentException("jobqueue.worker.heartbeat-interval must be positive");
        }
        // Invariant 5: with at least three heartbeats per lease, one or two late or failed heartbeats
        // do not make a healthy worker lose its jobs.
        if (lease.compareTo(heartbeatInterval.multipliedBy(3)) < 0) {
            throw new IllegalArgumentException("jobqueue.worker.lease (" + lease + ") must be at least 3 x "
                    + "jobqueue.worker.heartbeat-interval (" + heartbeatInterval + ")");
        }
    }

    // The random suffix keeps two workers on the same host (or a restarted one) from sharing an id:
    // locked_by is part of the fencing check, so ids must never be reused by a different process.
    private static String defaultWorkerId() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "worker";
        }
        return host + "-" + UUID.randomUUID().toString().substring(0, 6);
    }
}
