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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Isolation between queues (the bulkhead): each queue has its own slots, so a slow queue cannot starve a fast one.
 */
class QueueIsolationIntegrationTest extends PostgresIntegrationTest {

    /** Counts how many handlers of one kind run at the same time, and the most it ever saw. */
    static class Gauge {
        final AtomicInteger running = new AtomicInteger();
        final AtomicInteger max = new AtomicInteger();

        void enter() {
            max.accumulateAndGet(running.incrementAndGet(), Math::max);
        }

        void leave() {
            running.decrementAndGet();
        }
    }

    static class QueueHandlers {
        final Gauge slow = new Gauge();
        final Gauge fast = new Gauge();
        volatile CountDownLatch slowGate = new CountDownLatch(1); // closed: slow handlers block until opened

        @JobHandler("slow")
        public void slow(JobContext context, JsonNode payload) throws InterruptedException {
            slow.enter();
            try {
                slowGate.await();
            } finally {
                slow.leave();
            }
        }

        @JobHandler("fast")
        public void fast(JobContext context, JsonNode payload) throws InterruptedException {
            fast.enter();
            try {
                Thread.sleep(20); // a little work, so that fast jobs overlap when slots allow it
            } finally {
                fast.leave();
            }
        }
    }

    private final QueueHandlers handlers = new QueueHandlers();
    private AnnotationConfigApplicationContext spring;
    private JobPoller poller;

    @AfterEach
    void tearDown() {
        handlers.slowGate.countDown(); // never leave a handler parked
        if (poller != null) {
            poller.stop();
        }
        if (spring != null) {
            spring.close();
        }
    }

    private void start(Map<String, Integer> concurrency) {
        start(concurrency, Duration.ofMillis(50), Duration.ofMillis(100));
    }

    private void start(Map<String, Integer> concurrency, Duration pollInterval, Duration maxPollInterval) {
        spring = new AnnotationConfigApplicationContext();
        spring.registerBean(ObjectMapper.class, () -> JsonMapper.builder().build());
        spring.registerBean("queueHandlers", QueueHandlers.class, () -> handlers);
        spring.register(HandlerRegistry.class);
        spring.refresh();
        // No "queues" list and a default of 1 slot: the queues and their concurrency come from the map alone.
        WorkerProperties properties = new WorkerProperties("iso-worker", List.of(), 1, Duration.ofSeconds(30),
                pollInterval, maxPollInterval, Duration.ofMillis(100), Duration.ofSeconds(1),
                null, concurrency, null);
        poller = new JobPoller(new JobClaimRepository(JDBC), new JobCompletionRepository(JDBC),
                spring.getBean(HandlerRegistry.class), properties, new RunningJobs());
        poller.start();
    }

    private void insertJobs(String queue, String type, int count) {
        JDBC.sql("INSERT INTO jobs (queue, type) SELECT :q, :t FROM generate_series(1, :n)")
                .param("q", queue).param("t", type).param("n", count).update();
    }

    private int count(String queue, String status) {
        return JDBC.sql("SELECT count(*) FROM jobs WHERE queue = :q AND status = :s")
                .param("q", queue).param("s", status).query(Integer.class).single();
    }

    @Test
    void shouldKeepProcessingTheFastQueueWhileTheSlowQueueIsSaturated() {
        insertJobs("iso-slow", "slow", 10);
        insertJobs("iso-fast", "fast", 30);

        start(Map.of("iso-slow", 2, "iso-fast", 2));

        // The slow queue fills its 2 slots with handlers that do not return.
        await().atMost(Duration.ofSeconds(10)).until(() -> handlers.slow.running.get() == 2);

        // Meanwhile every fast job finishes: the saturated slow queue took none of the fast queue's slots.
        await().atMost(Duration.ofSeconds(15)).until(() -> count("iso-fast", "SUCCEEDED") == 30);
        assertThat(handlers.slow.running.get()).as("slow handlers still blocked").isEqualTo(2);
        assertThat(count("iso-slow", "RUNNING")).isEqualTo(2);
        assertThat(count("iso-slow", "QUEUED")).as("slow jobs still waiting").isEqualTo(8);

        // Open the gate: the slow queue drains too, never more than its own 2 at a time.
        handlers.slowGate.countDown();
        await().atMost(Duration.ofSeconds(15)).until(() -> count("iso-slow", "SUCCEEDED") == 10);
        assertThat(handlers.slow.max.get()).isEqualTo(2);
        assertThat(handlers.fast.max.get()).as("fast queue never exceeded its own 2 slots").isLessThanOrEqualTo(2);
    }

    @Test
    void shouldRunExactlyTheConfiguredNumberOfHandlersInEachQueue() {
        insertJobs("iso-big", "slow", 8);
        insertJobs("iso-small", "slow", 4);

        start(Map.of("iso-big", 3, "iso-small", 1));

        // Both queues use only their own limit, at the same time: 3 + 1 handlers running, not 4 from one queue.
        await().atMost(Duration.ofSeconds(10)).until(() -> handlers.slow.running.get() == 4);
        await().during(Duration.ofMillis(700)).atMost(Duration.ofSeconds(5)).until(() ->
                handlers.slow.running.get() == 4
                        && count("iso-big", "RUNNING") == 3 && count("iso-big", "QUEUED") == 5
                        && count("iso-small", "RUNNING") == 1 && count("iso-small", "QUEUED") == 3);

        handlers.slowGate.countDown();
        await().atMost(Duration.ofSeconds(15)).until(() ->
                count("iso-big", "SUCCEEDED") == 8 && count("iso-small", "SUCCEEDED") == 4);
        assertThat(handlers.slow.max.get()).isEqualTo(4);
    }

    @Test
    void shouldFreeASlotOfASaturatedQueueAsSoonAsAHandlerEnds() {
        // A queue with a single slot and a backlog: each finished job must be replaced by the next at once
        // (the wake-up when a slot is released), not after the poller's long wait.
        insertJobs("iso-one", "fast", 40);

        // The poller waits up to 5 s when it has nothing to do. If a freed slot went unnoticed for that long,
        // 40 jobs in one slot would take over 3 minutes; with the wake-up they take about a second.
        start(Map.of("iso-one", 1), Duration.ofSeconds(5), Duration.ofSeconds(5));

        await().atMost(Duration.ofSeconds(10)).until(() -> count("iso-one", "SUCCEEDED") == 40);
        assertThat(handlers.fast.max.get()).isEqualTo(1);
    }
}
