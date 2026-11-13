package dev.jobqueue.worker.heartbeat;

import dev.jobqueue.common.JobContext;
import dev.jobqueue.common.JobHandler;
import dev.jobqueue.worker.PostgresIntegrationTest;
import dev.jobqueue.worker.WorkerProperties;
import dev.jobqueue.worker.claim.JobClaimRepository;
import dev.jobqueue.worker.completion.JobCompletionRepository;
import dev.jobqueue.worker.handler.HandlerRegistry;
import dev.jobqueue.worker.poller.JobPoller;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Lease and heartbeat with the real poller. The lease is 3 s and the heartbeat 1 s (the minimum ratio of 3),
 * so a job that is not heartbeated expires after 3 s.
 */
class JobHeartbeatIntegrationTest extends PostgresIntegrationTest {

    private static final Duration LEASE = Duration.ofSeconds(3);
    private static final Duration HEARTBEAT = Duration.ofSeconds(1);

    static class BlockingHandler {
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch interrupted = new CountDownLatch(1);

        /** Runs until released; records an interrupt. */
        @JobHandler("block")
        public void block(JobContext context, JsonNode payload) throws InterruptedException {
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                interrupted.countDown();
                throw e;
            }
        }
    }

    private final BlockingHandler handler = new BlockingHandler();
    private AnnotationConfigApplicationContext spring;
    private JobPoller poller;
    private HeartbeatService heartbeat;

    @AfterEach
    void tearDown() {
        handler.release.countDown();
        if (heartbeat != null) {
            heartbeat.stop();
        }
        if (poller != null) {
            poller.stop();
        }
        if (spring != null) {
            spring.close();
        }
    }

    private void start(String queue, JobClaimRepository claimRepository) {
        spring = new AnnotationConfigApplicationContext();
        spring.registerBean(ObjectMapper.class, () -> JsonMapper.builder().build());
        spring.registerBean("blockingHandler", BlockingHandler.class, () -> handler);
        spring.register(HandlerRegistry.class);
        spring.refresh();
        WorkerProperties properties = new WorkerProperties("heartbeat-worker", List.of(queue), 1, LEASE,
                Duration.ofMillis(50), Duration.ofMillis(100), Duration.ofMillis(100), Duration.ofSeconds(1),
                HEARTBEAT);
        RunningJobs runningJobs = new RunningJobs();
        poller = new JobPoller(claimRepository, new JobCompletionRepository(JDBC),
                spring.getBean(HandlerRegistry.class), properties, runningJobs);
        heartbeat = new HeartbeatService(claimRepository, runningJobs, properties);
        poller.start();
        heartbeat.start();
    }

    private UUID insertJob(String queue) {
        return JDBC.sql("INSERT INTO jobs (queue, type) VALUES (:q, 'block') RETURNING id")
                .param("q", queue).query(UUID.class).single();
    }

    private boolean leaseStillValid(UUID id) {
        return JDBC.sql("SELECT status = 'RUNNING' AND locked_until > now() FROM jobs WHERE id = :id")
                .param("id", id).query(Boolean.class).single();
    }

    @Test
    void shouldKeepTheLeaseAliveWhileALongJobRunsWellBeyondTheLeaseDuration() throws Exception {
        UUID id = insertJob("hb-long");

        start("hb-long", new JobClaimRepository(JDBC));

        assertThat(handler.started.await(10, TimeUnit.SECONDS)).isTrue();
        // The job runs for 5 s, longer than the 3 s lease: without heartbeats the lease would have expired.
        await().during(Duration.ofSeconds(5)).atMost(Duration.ofSeconds(15)).until(() -> leaseStillValid(id));

        handler.release.countDown();
        await().atMost(Duration.ofSeconds(10)).until(() ->
                JDBC.sql("SELECT status FROM jobs WHERE id = :id").param("id", id).query(String.class).single()
                        .equals("SUCCEEDED"));
    }

    @Test
    void shouldInterruptTheHandlerAndWriteNothingWhenTheLeaseIsStolen() throws Exception {
        UUID id = insertJob("hb-stolen");
        start("hb-stolen", new JobClaimRepository(JDBC));
        assertThat(handler.started.await(10, TimeUnit.SECONDS)).isTrue();

        // Another worker takes the job over (as the reaper and a new claim would after an expiry).
        JDBC.sql("UPDATE jobs SET locked_by = 'thief', attempts = attempts + 1 WHERE id = :id").param("id", id).update();

        // The next heartbeat finds the job missing from its result and cancels the handler.
        assertThat(handler.interrupted.await(10, TimeUnit.SECONDS))
                .as("handler interrupted after the lease was lost").isTrue();
        // The thief's row is untouched, and this worker recorded nothing for it.
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5)).until(() ->
                JDBC.sql("SELECT status || '|' || locked_by || '|' || attempts FROM jobs WHERE id = :id")
                        .param("id", id).query(String.class).single().equals("RUNNING|thief|2"));
        Integer attemptRows = JDBC.sql("SELECT count(*) FROM job_attempts WHERE job_id = :id")
                .param("id", id).query(Integer.class).single();
        assertThat(attemptRows).isZero();
    }

    @Test
    void shouldKeepHeartbeatingAfterOneHeartbeatFails() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        JobClaimRepository failsOnce = new JobClaimRepository(JDBC) {
            @Override
            public Set<UUID> extendLeases(String workerId, Map<UUID, Integer> attemptsById,
                    Duration lease) {
                if (calls.incrementAndGet() == 1) {
                    throw new IllegalStateException("simulated database hiccup");
                }
                return super.extendLeases(workerId, attemptsById, lease);
            }
        };
        UUID id = insertJob("hb-hiccup");
        start("hb-hiccup", failsOnce);
        assertThat(handler.started.await(10, TimeUnit.SECONDS)).isTrue();

        // Beat 1 fails, but the lease (3 s) outlives one missed 1 s heartbeat, and later beats keep it alive.
        await().during(Duration.ofSeconds(5)).atMost(Duration.ofSeconds(15)).until(() -> leaseStillValid(id));
        assertThat(calls.get()).as("heartbeats attempted, including the one that failed").isGreaterThan(2);
    }
}
