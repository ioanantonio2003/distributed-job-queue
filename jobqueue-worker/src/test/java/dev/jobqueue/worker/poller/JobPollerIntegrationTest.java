package dev.jobqueue.worker.poller;

import dev.jobqueue.common.JobContext;
import dev.jobqueue.common.JobHandler;
import dev.jobqueue.worker.PostgresIntegrationTest;
import dev.jobqueue.worker.WorkerProperties;
import dev.jobqueue.worker.claim.JobClaimRepository;
import dev.jobqueue.worker.handler.HandlerRegistry;
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
                Duration.ofSeconds(30), Duration.ofMillis(50), Duration.ofMillis(200));
        poller = new JobPoller(new JobClaimRepository(JDBC), spring.getBean(HandlerRegistry.class), properties);
        poller.start();
        return poller;
    }

    private void insertJobs(String queue, String type, int count) {
        JDBC.sql("INSERT INTO jobs (queue, type) SELECT :q, :t FROM generate_series(1, :n)")
                .param("q", queue).param("t", type).param("n", count).update();
    }

    private int countByStatus(String queue, String status) {
        return JDBC.sql("SELECT count(*) FROM jobs WHERE queue = :q AND status = :s")
                .param("q", queue).param("s", status).query(Integer.class).single();
    }

    @Test
    void shouldRunHandlerForEveryQueuedJobWhenPollerIsRunning() {
        insertJobs("poll-all", "record", 3);

        startPoller("poll-all", 2);

        await().atMost(Duration.ofSeconds(10)).until(() -> handlers.handled.size() == 3);
        assertThat(handlers.handled).doesNotHaveDuplicates();
        // Completion is Step 10: until then every executed job stays RUNNING with attempts = 1.
        assertThat(countByStatus("poll-all", "RUNNING")).isEqualTo(3);
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
        await().atMost(Duration.ofSeconds(10)).until(() -> handlers.handled.size() == 10);
        assertThat(handlers.maxRunning.get()).isEqualTo(3);
    }

    @Test
    void shouldKeepWorkingAndFreeTheSlotWhenHandlerFailsOrTypeIsUnknown() {
        insertJobs("poll-fail", "fail", 1);
        insertJobs("poll-fail", "no-such-type", 1);
        insertJobs("poll-fail", "record", 1);

        // One slot: if a failed job leaked its slot, the "record" job could never run.
        startPoller("poll-fail", 1);

        await().atMost(Duration.ofSeconds(10)).until(() -> handlers.handled.size() == 1);
        assertThat(poller.isRunning()).isTrue();
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
