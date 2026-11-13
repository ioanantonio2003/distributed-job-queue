package dev.jobqueue.worker.poller;

import dev.jobqueue.common.JobContext;
import dev.jobqueue.common.JobHandler;
import dev.jobqueue.worker.PostgresIntegrationTest;
import dev.jobqueue.worker.WorkerProperties;
import dev.jobqueue.worker.claim.JobClaimRepository;
import dev.jobqueue.worker.completion.JobCompletionRepository;
import dev.jobqueue.worker.handler.HandlerRegistry;
import dev.jobqueue.worker.heartbeat.RunningJobs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class JobPollerIntegrationTest extends PostgresIntegrationTest {

    static class TestHandlers {
        final Queue<UUID> handled = new ConcurrentLinkedQueue<>();
        final AtomicInteger running = new AtomicInteger();
        final AtomicInteger maxRunning = new AtomicInteger();
        volatile CountDownLatch gate = new CountDownLatch(0); // open by default

        @JobHandler("record")
        public void record(JobContext context, JsonNode payload) throws InterruptedException {
            int now = running.incrementAndGet();
            maxRunning.accumulateAndGet(now, Math::max);
            try {
                gate.await();
                handled.add(context.jobId());
            } finally {
                running.decrementAndGet();
            }
        }

        @JobHandler("fail")
        public void fail(JobContext context, JsonNode payload) {
            throw new IllegalStateException("boom");
        }
    }

    private final TestHandlers handlers = new TestHandlers();
    private AnnotationConfigApplicationContext spring;
    private JobPoller poller;

    @AfterEach
    void tearDown() {
        handlers.gate.countDown(); // never leave virtual threads parked
        if (poller != null) {
            poller.stop();
        }
        if (spring != null) {
            spring.close();
        }
    }

    private JobPoller startPoller(String queue, int slots) {
        spring = new AnnotationConfigApplicationContext();
        spring.registerBean(ObjectMapper.class, () -> JsonMapper.builder().build());
        spring.registerBean("testHandlers", TestHandlers.class, () -> handlers);
        spring.register(HandlerRegistry.class);
        spring.refresh();

        WorkerProperties properties = new WorkerProperties("test-worker", List.of(queue), slots,
                Duration.ofSeconds(30), Duration.ofMillis(50), Duration.ofMillis(200),
                Duration.ofMillis(50), Duration.ofMillis(200), null);
        poller = new JobPoller(new JobClaimRepository(JDBC), new JobCompletionRepository(JDBC),
                spring.getBean(HandlerRegistry.class), properties, new RunningJobs());
        poller.start();
        return poller;
    }

    private void insertJobs(String queue, String type, int count) {
        insertJobs(queue, type, count, 5);
    }

    private void insertJobs(String queue, String type, int count, int maxAttempts) {
        JDBC.sql("INSERT INTO jobs (queue, type, max_attempts) SELECT :q, :t, :m FROM generate_series(1, :n)")
                .param("q", queue).param("t", type).param("m", maxAttempts).param("n", count).update();
    }

    private int countByStatus(String queue, String status) {
        return JDBC.sql("SELECT count(*) FROM jobs WHERE queue = :q AND status = :s")
                .param("q", queue).param("s", status).query(Integer.class).single();
    }

    @Test
    void shouldRunHandlerAndMarkSucceededForEveryQueuedJobWhenPollerIsRunning() {
        insertJobs("poll-all", "record", 3);

        startPoller("poll-all", 2);

        await().atMost(Duration.ofSeconds(10)).until(() -> countByStatus("poll-all", "SUCCEEDED") == 3);
        assertThat(handlers.handled).hasSize(3).doesNotHaveDuplicates();
        Integer attemptRows = JDBC.sql("""
                        SELECT count(*) FROM job_attempts a JOIN jobs j ON j.id = a.job_id
                        WHERE j.queue = 'poll-all' AND a.outcome = 'SUCCEEDED' AND a.attempt = 1
                        """)
                .query(Integer.class).single();
        assertThat(attemptRows).isEqualTo(3);
    }

    @Test
    void shouldNeverRunMoreHandlersThanSlotsWhenHandlersAreBlocked() {
        handlers.gate = new CountDownLatch(1); // handlers block until we open the gate
        insertJobs("poll-slots", "record", 10);

        startPoller("poll-slots", 3);

        await().atMost(Duration.ofSeconds(10)).until(() -> handlers.running.get() == 3);
        // The slots stay full, and the other 7 jobs stay untouched in the queue, for a while.
        await().during(Duration.ofMillis(700)).atMost(Duration.ofSeconds(5)).until(() ->
                handlers.running.get() == 3
                        && countByStatus("poll-slots", "RUNNING") == 3
                        && countByStatus("poll-slots", "QUEUED") == 7);

        handlers.gate.countDown();

        // Freed slots are reused until all 10 are done, never exceeding 3 at once.
        await().atMost(Duration.ofSeconds(10)).until(() -> countByStatus("poll-slots", "SUCCEEDED") == 10);
        assertThat(handlers.maxRunning.get()).isEqualTo(3);
    }

    @Test
    void shouldKeepWorkingAndFreeTheSlotWhenHandlerFailsOrTypeIsUnknown() {
        // max_attempts = 1: the first failure is final, so the failed jobs end up DEAD instead of being retried.
        insertJobs("poll-fail", "fail", 1, 1);
        insertJobs("poll-fail", "no-such-type", 1, 1);
        insertJobs("poll-fail", "record", 1);

        // One slot: if a failed job leaked its slot, the "record" job could never run.
        startPoller("poll-fail", 1);

        await().atMost(Duration.ofSeconds(10)).until(() -> countByStatus("poll-fail", "SUCCEEDED") == 1);
        assertThat(poller.isRunning()).isTrue();
        await().atMost(Duration.ofSeconds(10)).until(() -> countByStatus("poll-fail", "DEAD") == 2);
        assertThat(countByStatus("poll-fail", "RUNNING")).isZero();
    }

    @Test
    void shouldIgnoreTheResultWhenTheJobWasTakenAwayWhileTheHandlerRan() {
        handlers.gate = new CountDownLatch(1);
        insertJobs("poll-stolen", "record", 1);
        startPoller("poll-stolen", 1);
        await().atMost(Duration.ofSeconds(10)).until(() -> handlers.running.get() == 1);

        // Another worker "steals" the job (as after a lease expiry) while our handler is still running.
        JDBC.sql("UPDATE jobs SET locked_by = 'thief', attempts = attempts + 1 WHERE queue = 'poll-stolen'").update();
        handlers.gate.countDown();

        await().atMost(Duration.ofSeconds(10)).until(() -> handlers.handled.size() == 1);
        // The handler finished, but its completion was fenced out: the job is still the thief's.
        await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(10)).until(() ->
                countByStatus("poll-stolen", "RUNNING") == 1);
        Integer attemptRows = JDBC.sql("""
                        SELECT count(*) FROM job_attempts a JOIN jobs j ON j.id = a.job_id WHERE j.queue = 'poll-stolen'
                        """).query(Integer.class).single();
        assertThat(attemptRows).isZero();
    }

    @Test
    void shouldStopPollingWhenStopped() {
        startPoller("poll-stop", 2);
        poller.stop();

        insertJobs("poll-stop", "record", 1);

        await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(3)).until(() ->
                handlers.handled.isEmpty() && countByStatus("poll-stop", "QUEUED") == 1);
        assertThat(poller.isRunning()).isFalse();
    }
}
