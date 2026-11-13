package dev.jobqueue.worker.poller;

import dev.jobqueue.common.JobContext;
import dev.jobqueue.common.JobHandler;
import dev.jobqueue.worker.PostgresIntegrationTest;
import dev.jobqueue.worker.WorkerProperties;
import dev.jobqueue.worker.claim.JobClaimRepository;
import dev.jobqueue.worker.completion.JobCompletionRepository;
import dev.jobqueue.worker.demo.FailureDemoHandlers;
import dev.jobqueue.worker.handler.HandlerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Job time limits, with the real poller. The jobs here have timeout_seconds = 1, the smallest the API allows.
 */
class JobTimeoutIntegrationTest extends PostgresIntegrationTest {

    static class TimeoutHandlers {
        final CountDownLatch interrupted = new CountDownLatch(1);
        final CountDownLatch quickRan = new CountDownLatch(1);
        final AtomicLong spins = new AtomicLong();
        volatile boolean stopStubborn;

        /** Like "hang", but records that the interrupt arrived. */
        @JobHandler("watch")
        public void watch(JobContext context, JsonNode payload) throws InterruptedException {
            try {
                Thread.sleep(Duration.ofHours(1));
            } catch (InterruptedException e) {
                interrupted.countDown();
                throw e;
            }
        }

        /** Busy-loops and ignores interrupts: the kind of code a timeout cannot stop. */
        @JobHandler("stubborn")
        public void stubborn(JobContext context, JsonNode payload) {
            while (!stopStubborn) {
                spins.incrementAndGet();
                Thread.onSpinWait();
            }
        }

        @JobHandler("quick")
        public void quick(JobContext context, JsonNode payload) {
            quickRan.countDown();
        }
    }

    private final TimeoutHandlers handlers = new TimeoutHandlers();
    private AnnotationConfigApplicationContext spring;
    private JobPoller poller;

    @AfterEach
    void tearDown() {
        handlers.stopStubborn = true; // let the busy-looping thread end, or it would burn a core until the JVM exits
        if (poller != null) {
            poller.stop();
        }
        if (spring != null) {
            spring.close();
        }
    }

    private void startPoller(String queue, int slots) {
        spring = new AnnotationConfigApplicationContext();
        spring.registerBean(ObjectMapper.class, () -> JsonMapper.builder().build());
        spring.registerBean("timeoutHandlers", TimeoutHandlers.class, () -> handlers);
        spring.register(HandlerRegistry.class, FailureDemoHandlers.class);
        spring.refresh();
        WorkerProperties properties = new WorkerProperties("timeout-worker", List.of(queue), slots,
                Duration.ofSeconds(30), Duration.ofMillis(50), Duration.ofMillis(100),
                Duration.ofMillis(100), Duration.ofSeconds(1));
        poller = new JobPoller(new JobClaimRepository(JDBC), new JobCompletionRepository(JDBC),
                spring.getBean(HandlerRegistry.class), properties);
        poller.start();
    }

    private UUID insertJob(String queue, String type, int priority, int maxAttempts) {
        return JDBC.sql("""
                        INSERT INTO jobs (queue, type, priority, max_attempts, timeout_seconds)
                        VALUES (:q, :t, :p, :m, 1) RETURNING id
                        """)
                .param("q", queue).param("t", type).param("p", priority).param("m", maxAttempts)
                .query(UUID.class).single();
    }

    private String status(UUID id) {
        return JDBC.sql("SELECT status FROM jobs WHERE id = :id").param("id", id).query(String.class).single();
    }

    private List<String> outcomes(UUID id) {
        return JDBC.sql("SELECT outcome FROM job_attempts WHERE job_id = :id ORDER BY attempt")
                .param("id", id).query(String.class).list();
    }

    @Test
    void shouldInterruptTheHandlerAndRecordTimedOutWhenTheLimitIsExceeded() throws Exception {
        UUID id = insertJob("timeout-interrupt", "watch", 0, 1);

        startPoller("timeout-interrupt", 1);

        await().atMost(Duration.ofSeconds(15)).until(() -> status(id).equals("DEAD"));
        assertThat(handlers.interrupted.await(5, TimeUnit.SECONDS))
                .as("the handler thread received the interrupt").isTrue();
        assertThat(outcomes(id)).containsExactly("TIMED_OUT");
        String error = JDBC.sql("SELECT last_error FROM jobs WHERE id = :id").param("id", id)
                .query(String.class).single();
        assertThat(error).isEqualTo("TimeoutException: handler exceeded timeout of 1 s");
    }

    @Test
    void shouldRetryTheJobWhenItTimesOutAndAttemptsRemain() {
        UUID id = insertJob("timeout-retry", "hang", 0, 2);

        startPoller("timeout-retry", 1);

        await().atMost(Duration.ofSeconds(20)).until(() -> status(id).equals("DEAD"));
        // Timed out twice: the first one was retried (QUEUED), the second used the last attempt (DEAD).
        assertThat(outcomes(id)).containsExactly("TIMED_OUT", "TIMED_OUT");
    }

    @Test
    void shouldFreeTheSlotWhenAHandlerTimesOut() {
        // One slot. The hanging job has the higher priority, so it is claimed first and blocks the only slot;
        // the quick job can only run once the timeout has freed it.
        UUID hanging = insertJob("timeout-slot", "hang", 1, 1);
        insertJob("timeout-slot", "quick", 0, 1);

        startPoller("timeout-slot", 1);

        await().atMost(Duration.ofSeconds(15)).until(() -> handlers.quickRan.getCount() == 0);
        assertThat(status(hanging)).isEqualTo("DEAD");
    }

    @Test
    void shouldStillFreeTheSlotWhenTheHandlerIgnoresTheInterrupt() {
        // The documented limitation: the timeout is recorded and the slot is freed, but a handler that never
        // checks its interrupt flag keeps running in the background (we stop it in tearDown).
        UUID stubborn = insertJob("timeout-stubborn", "stubborn", 1, 1);
        insertJob("timeout-stubborn", "quick", 0, 1);

        startPoller("timeout-stubborn", 1);

        await().atMost(Duration.ofSeconds(15)).until(() -> handlers.quickRan.getCount() == 0);
        assertThat(status(stubborn)).isEqualTo("DEAD");
        assertThat(outcomes(stubborn)).containsExactly("TIMED_OUT");
        // Proof that it was not stopped: its loop counter keeps growing after the timeout was recorded.
        long spinsNow = handlers.spins.get();
        await().atMost(Duration.ofSeconds(5)).until(() -> handlers.spins.get() > spinsNow);
    }
}
