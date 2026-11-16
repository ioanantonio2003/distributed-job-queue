package dev.jobqueue.worker.poller;

import dev.jobqueue.common.JobContext;
import dev.jobqueue.common.JobHandler;
import dev.jobqueue.worker.PostgresIntegrationTest;
import dev.jobqueue.worker.WorkerProperties;
import dev.jobqueue.worker.claim.ClaimedJob;
import dev.jobqueue.worker.claim.JobClaimRepository;
import dev.jobqueue.worker.completion.JobCompletionRepository;
import dev.jobqueue.worker.handler.HandlerRegistry;
import dev.jobqueue.worker.heartbeat.HeartbeatService;
import dev.jobqueue.worker.heartbeat.RunningJobs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Graceful shutdown with the real poller: DRAINING, finishing short jobs, giving back long ones.
 * {@code poller.stop()} is what Spring calls on SIGTERM, so these tests exercise the same code path.
 */
class GracefulShutdownIntegrationTest extends PostgresIntegrationTest {

    static class ShutdownHandlers {
        final AtomicInteger shortRuns = new AtomicInteger();
        final CountDownLatch shortStarted = new CountDownLatch(1);
        final CountDownLatch holdStarted = new CountDownLatch(1);
        final CountDownLatch holdInterrupted = new CountDownLatch(1);
        final CountDownLatch releaseHold = new CountDownLatch(1);

        /** A job that is nearly done: finishes by itself in well under the grace period. */
        @JobHandler("short")
        public void shortJob(JobContext context, JsonNode payload) throws InterruptedException {
            shortRuns.incrementAndGet();
            shortStarted.countDown();
            Thread.sleep(600);
        }

        /** A long job: runs until released, and records being interrupted. */
        @JobHandler("hold")
        public void hold(JobContext context, JsonNode payload) throws InterruptedException {
            holdStarted.countDown();
            try {
                releaseHold.await();
            } catch (InterruptedException e) {
                holdInterrupted.countDown();
                throw e;
            }
        }
    }

    private final ShutdownHandlers handlers = new ShutdownHandlers();
    private AnnotationConfigApplicationContext spring;
    private JobPoller poller;
    private HeartbeatService heartbeat;

    @AfterEach
    void tearDown() {
        handlers.releaseHold.countDown();
        if (poller != null) {
            poller.stop();
        }
        if (heartbeat != null) {
            heartbeat.stop();
        }
        if (spring != null) {
            spring.close();
        }
    }

    private void start(String queue, Duration grace, boolean withHeartbeat) {
        spring = new AnnotationConfigApplicationContext();
        spring.registerBean(ObjectMapper.class, () -> JsonMapper.builder().build());
        spring.registerBean("shutdownHandlers", ShutdownHandlers.class, () -> handlers);
        spring.register(HandlerRegistry.class);
        spring.refresh();
        WorkerProperties properties = new WorkerProperties("shutdown-worker", List.of(queue), 2,
                Duration.ofSeconds(3), Duration.ofMillis(50), Duration.ofMillis(100), Duration.ofMillis(100),
                Duration.ofSeconds(1), null, null, grace);
        RunningJobs runningJobs = new RunningJobs();
        JobClaimRepository claimRepository = new JobClaimRepository(JDBC);
        poller = new JobPoller(claimRepository, new JobCompletionRepository(JDBC),
                spring.getBean(HandlerRegistry.class), properties, runningJobs);
        if (withHeartbeat) {
            heartbeat = new HeartbeatService(claimRepository, runningJobs, properties);
            heartbeat.start();
        }
        poller.start();
    }

    private UUID insertJob(String queue, String type) {
        return JDBC.sql("INSERT INTO jobs (queue, type) VALUES (:q, :t) RETURNING id")
                .param("q", queue).param("t", type).query(UUID.class).single();
    }

    private String row(UUID id) {
        return JDBC.sql("""
                        SELECT status || '|' || coalesce(locked_by, '-') || '|' || attempts || '|' || (locked_until IS NULL)
                        FROM jobs WHERE id = :id
                        """).param("id", id).query(String.class).single();
    }

    private int historyRows(UUID id) {
        return JDBC.sql("SELECT count(*) FROM job_attempts WHERE job_id = :id").param("id", id)
                .query(Integer.class).single();
    }

    @Test
    void shouldLetAShortJobFinishAndNotRunItAgainWhenStoppedWhileItRuns() throws Exception {
        UUID id = insertJob("shut-short", "short");
        start("shut-short", Duration.ofSeconds(20), false);
        assertThat(handlers.shortStarted.await(10, TimeUnit.SECONDS)).isTrue();

        poller.stop(); // returns only after the running job has finished (it is much shorter than the grace)

        assertThat(row(id)).isEqualTo("SUCCEEDED|-|1|true");
        assertThat(historyRows(id)).isEqualTo(1);
        // Nothing runs it a second time afterwards.
        await().during(Duration.ofMillis(700)).atMost(Duration.ofSeconds(5)).until(() ->
                handlers.shortRuns.get() == 1 && row(id).startsWith("SUCCEEDED"));
    }

    @Test
    void shouldGiveALongJobBackWithoutUsingAnAttemptWhenTheGracePeriodEnds() throws Exception {
        UUID id = insertJob("shut-long", "hold");
        start("shut-long", Duration.ofMillis(500), false);
        assertThat(handlers.holdStarted.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(row(id)).startsWith("RUNNING|shutdown-worker|1|"); // claimed: attempts went 0 -> 1

        poller.stop(); // waits 500 ms, interrupts the handler, gives the job back

        assertThat(handlers.holdInterrupted.await(5, TimeUnit.SECONDS)).as("handler was interrupted").isTrue();
        // Back in the queue, unlocked, with attempts as before the claim, and no history row for the unfinished attempt.
        assertThat(row(id)).isEqualTo("QUEUED|-|0|true");
        assertThat(historyRows(id)).isZero();

        // Another worker picks it up: it is attempt 1 again, because the first one was given back.
        List<ClaimedJob> claimed = new JobClaimRepository(JDBC).claim("next-worker", "shut-long",
                Duration.ofSeconds(30), 1);
        assertThat(claimed).singleElement().satisfies(job -> {
            assertThat(job.id()).isEqualTo(id);
            assertThat(job.attempt()).isEqualTo(1);
        });
    }

    @Test
    void shouldNotClaimNewJobsWhileDraining() throws Exception {
        UUID running = insertJob("shut-drain", "hold");
        start("shut-drain", Duration.ofSeconds(20), false);
        assertThat(handlers.holdStarted.await(10, TimeUnit.SECONDS)).isTrue();

        CompletableFuture<Void> stopping = CompletableFuture.runAsync(poller::stop); // blocks: draining
        UUID late = insertJob("shut-drain", "short");

        // While draining, the running job is untouched and the new one is left in the queue.
        await().during(Duration.ofMillis(800)).atMost(Duration.ofSeconds(5)).until(() ->
                row(running).startsWith("RUNNING") && row(late).startsWith("QUEUED") && !stopping.isDone());

        handlers.releaseHold.countDown(); // the running job finishes
        stopping.get(10, TimeUnit.SECONDS);

        assertThat(row(running)).startsWith("SUCCEEDED");
        assertThat(row(late)).as("the job that arrived during the drain").isEqualTo("QUEUED|-|0|true");
        assertThat(handlers.shortRuns.get()).isZero();
    }

    @Test
    void shouldKeepTheLeaseAliveWhileDraining() throws Exception {
        UUID id = insertJob("shut-lease", "hold");
        start("shut-lease", Duration.ofSeconds(20), true); // lease 3 s, heartbeat 1 s
        assertThat(handlers.holdStarted.await(10, TimeUnit.SECONDS)).isTrue();

        CompletableFuture<Void> stopping = CompletableFuture.runAsync(poller::stop);

        // The drain lasts longer than the 3 s lease. The job is still ours because the heartbeat keeps running
        // until the poller is done (it has a lower lifecycle phase, so Spring stops it later).
        await().during(Duration.ofSeconds(4)).atMost(Duration.ofSeconds(12)).until(() ->
                JDBC.sql("SELECT status = 'RUNNING' AND locked_until > now() FROM jobs WHERE id = :id")
                        .param("id", id).query(Boolean.class).single() && !stopping.isDone());

        handlers.releaseHold.countDown();
        stopping.get(10, TimeUnit.SECONDS);
        assertThat(row(id)).startsWith("SUCCEEDED");
    }

    @Test
    void shouldStopAtOnceAndBeIdempotentWhenNothingIsRunning() {
        start("shut-idle", Duration.ofSeconds(20), false);

        long startedAt = System.nanoTime();
        poller.stop();
        poller.stop(); // a second stop is harmless
        Duration took = Duration.ofNanos(System.nanoTime() - startedAt);

        assertThat(took).isLessThan(Duration.ofSeconds(3));
        assertThat(poller.isRunning()).isFalse();
    }

    @Test
    void shouldStopThePollerBeforeTheHeartbeatWhenTheApplicationShutsDown() {
        start("shut-phase", Duration.ofSeconds(20), true);

        // Spring stops the highest phase first: the poller drains while the heartbeat is still alive.
        assertThat(poller.getPhase()).isGreaterThan(heartbeat.getPhase());
    }
}
