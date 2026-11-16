package dev.jobqueue.server.reliability;

import com.zaxxer.hikari.HikariDataSource;
import dev.jobqueue.common.JobContext;
import dev.jobqueue.common.JobHandler;
import dev.jobqueue.server.PostgresIntegrationTest;
import dev.jobqueue.server.reaper.ReaperProperties;
import dev.jobqueue.server.reaper.ReaperRepository;
import dev.jobqueue.server.reaper.ReaperRepository.ReapResult;
import dev.jobqueue.server.reaper.ReaperService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.JsonNode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Deterministic reliability scenarios: real worker engines, the real reaper, one JVM, a real PostgreSQL.
 *
 * <p>The reaper is driven by hand ({@code reapAll()}) at the exact moment each scenario needs it, and handlers
 * block on latches, so every step happens in a known order even though the system is concurrent.
 * The lease is 3 s everywhere: the shortest value the 3 x heartbeat rule allows with a 1 s heartbeat.
 */
class ReliabilityScenariosIntegrationTest extends PostgresIntegrationTest {

    private static final Duration LEASE = Duration.ofSeconds(3);

    private final List<TestWorkerEngine> engines = new ArrayList<>();
    private final List<CountDownLatch> latchesToOpen = new ArrayList<>();
    private HikariDataSource reaperPool;
    private ReaperService reaper;

    @BeforeEach
    void setUp() {
        // The reaper looks at every RUNNING job in the database; start from a clean slate.
        JDBC.sql("DELETE FROM jobs WHERE status = 'RUNNING'").update();
        reaperPool = newPool(2);
        reaper = new ReaperService(new ReaperRepository(JdbcClient.create(reaperPool)),
                new ReaperProperties(Duration.ofSeconds(10), 100), new SimpleMeterRegistry());
    }

    @AfterEach
    void tearDown() {
        latchesToOpen.forEach(CountDownLatch::countDown); // never leave a handler parked
        engines.forEach(TestWorkerEngine::shutdown);
        reaperPool.close();
    }

    private CountDownLatch latch() {
        CountDownLatch latch = new CountDownLatch(1);
        latchesToOpen.add(latch);
        return latch;
    }

    private TestWorkerEngine engine(String workerId, String queue, int slots, boolean heartbeat, Object handlers) {
        TestWorkerEngine engine = new TestWorkerEngine(workerId, queue, slots, LEASE, heartbeat, newPool(3), handlers);
        engines.add(engine);
        return engine;
    }

    private UUID insertJob(String queue, String type, int maxAttempts) {
        return JDBC.sql("INSERT INTO jobs (queue, type, max_attempts) VALUES (:q, :t, :m) RETURNING id")
                .param("q", queue).param("t", type).param("m", maxAttempts).query(UUID.class).single();
    }

    private String status(UUID id) {
        return JDBC.sql("SELECT status FROM jobs WHERE id = :id").param("id", id).query(String.class).single();
    }

    private int attempts(UUID id) {
        return JDBC.sql("SELECT attempts FROM jobs WHERE id = :id").param("id", id).query(Integer.class).single();
    }

    private boolean leaseExpired(UUID id) {
        return JDBC.sql("SELECT status = 'RUNNING' AND locked_until < now() FROM jobs WHERE id = :id")
                .param("id", id).query(Boolean.class).single();
    }

    /** The job's history as "attempt:outcome:worker" strings, in attempt order. */
    private List<String> history(UUID id) {
        return JDBC.sql("SELECT attempt || ':' || outcome || ':' || worker_id FROM job_attempts "
                        + "WHERE job_id = :id ORDER BY attempt")
                .param("id", id).query(String.class).list();
    }

    // ---------------------------------------------------------------------------------------------------------

    static class ZombieHandlers {
        final CountDownLatch frozenWorkerStarted = new CountDownLatch(1);
        final CountDownLatch thaw;

        ZombieHandlers(CountDownLatch thaw) {
            this.thaw = thaw;
        }

        /** The first attempt hangs (a frozen worker); every later attempt finishes at once. */
        @JobHandler("zombie")
        public void run(JobContext context, JsonNode payload) throws InterruptedException {
            if (context.attempt() == 1) {
                frozenWorkerStarted.countDown();
                thaw.await();
            }
        }
    }

    @Test
    void shouldRejectTheResultOfAZombieWorkerThatWakesUpAfterItsJobWasTakenAndFinished() throws Exception {
        ZombieHandlers handlers = new ZombieHandlers(latch());
        UUID id = insertJob("rel-zombie", "zombie", 5);

        // Worker A claims the job, then "freezes": its heartbeat never runs and its handler is parked.
        TestWorkerEngine a = engine("zombie-A", "rel-zombie", 1, false, handlers);
        a.start();
        assertThat(handlers.frozenWorkerStarted.await(10, TimeUnit.SECONDS)).isTrue();

        // Its lease runs out; the reaper takes the job back.
        await().atMost(Duration.ofSeconds(15)).until(() -> leaseExpired(id));
        assertThat(reaper.reapAll()).isEqualTo(new ReapResult(1, 0));

        // Worker B (alive, heartbeating) claims it as attempt 2 and finishes it.
        TestWorkerEngine b = engine("zombie-B", "rel-zombie", 1, true, handlers);
        b.start();
        await().atMost(Duration.ofSeconds(15)).until(() -> status(id).equals("SUCCEEDED"));

        // A wakes up and tries to complete its old attempt: rejected.
        handlers.thaw.countDown();
        await().atMost(Duration.ofSeconds(10)).until(() -> !a.completionResults().isEmpty());
        assertThat(a.completionResults()).as("A's completion was accepted?").containsExactly(false);

        assertThat(status(id)).isEqualTo("SUCCEEDED");
        assertThat(attempts(id)).isEqualTo(2);
        assertThat(history(id)).containsExactly("1:LEASE_EXPIRED:zombie-A", "2:SUCCEEDED:zombie-B");
    }

    // ---------------------------------------------------------------------------------------------------------

    static class PoisonHandlers {
        final AtomicInteger poisonRuns = new AtomicInteger();

        @JobHandler("poison")
        public void poison(JobContext context, JsonNode payload) {
            poisonRuns.incrementAndGet();
            throw new IllegalStateException("poison pill");
        }

        @JobHandler("ok")
        public void ok(JobContext context, JsonNode payload) {
        }
    }

    @Test
    void shouldKillThePoisonPillAfterMaxAttemptsWhileTheOtherJobsKeepFlowing() {
        PoisonHandlers handlers = new PoisonHandlers();
        UUID poison = insertJob("rel-poison", "poison", 3);
        List<UUID> healthy = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            healthy.add(insertJob("rel-poison", "ok", 5));
        }

        engine("poison-worker", "rel-poison", 2, true, handlers).start();

        await().atMost(Duration.ofSeconds(20)).until(() -> status(poison).equals("DEAD")
                && healthy.stream().allMatch(id -> status(id).equals("SUCCEEDED")));
        assertThat(attempts(poison)).isEqualTo(3);
        assertThat(handlers.poisonRuns.get()).isEqualTo(3);
        assertThat(history(poison)).containsExactly(
                "1:FAILED:poison-worker", "2:FAILED:poison-worker", "3:FAILED:poison-worker");
    }

    // ---------------------------------------------------------------------------------------------------------

    static class KillerHandlers {
        final AtomicInteger started = new AtomicInteger();
        final CountDownLatch never;

        KillerHandlers(CountDownLatch never) {
            this.never = never;
        }

        /** A job that kills its worker: it never returns, and the test then crashes the whole engine. */
        @JobHandler("killer")
        public void run(JobContext context, JsonNode payload) throws InterruptedException {
            started.incrementAndGet();
            never.await();
        }
    }

    @Test
    void shouldEndUpDeadWhenAJobKeepsKillingItsWorkerBecauseACrashCountsAsAnAttempt() {
        KillerHandlers handlers = new KillerHandlers(latch());
        UUID id = insertJob("rel-killer", "killer", 3);

        for (int round = 1; round <= 3; round++) {
            TestWorkerEngine doomed = engine("doomed-" + round, "rel-killer", 1, true, handlers);
            doomed.start();
            int expectedStarts = round;
            await().atMost(Duration.ofSeconds(15)).until(() -> handlers.started.get() == expectedStarts);

            doomed.crash(); // the process dies with the job in its hands
            await().atMost(Duration.ofSeconds(15)).until(() -> leaseExpired(id));
            reaper.reapAll();
        }

        // Three crashes used up the three attempts: the job is dead-lettered instead of killing workers forever.
        assertThat(status(id)).isEqualTo("DEAD");
        assertThat(attempts(id)).isEqualTo(3);
        assertThat(history(id)).containsExactly(
                "1:LEASE_EXPIRED:doomed-1", "2:LEASE_EXPIRED:doomed-2", "3:LEASE_EXPIRED:doomed-3");
    }

    // ---------------------------------------------------------------------------------------------------------

    static class HoldHandlers {
        final AtomicInteger running = new AtomicInteger();
        final CountDownLatch hold;

        HoldHandlers(CountDownLatch hold) {
            this.hold = hold;
        }

        @JobHandler("hold")
        public void run(JobContext context, JsonNode payload) throws InterruptedException {
            running.incrementAndGet();
            try {
                hold.await();
            } finally {
                running.decrementAndGet();
            }
        }
    }

    @Test
    void shouldRecoverEveryInFlightJobAndOnlyThoseWhenTheEngineCrashesSuddenly() {
        CountDownLatch hold = latch();
        HoldHandlers handlers = new HoldHandlers(hold);
        List<UUID> jobs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            jobs.add(insertJob("rel-crash", "hold", 5));
        }

        // The engine has 4 slots: 4 jobs are in flight (blocked), 6 are still waiting in the queue.
        TestWorkerEngine doomed = engine("crashing", "rel-crash", 4, true, handlers);
        doomed.start();
        await().atMost(Duration.ofSeconds(10)).until(() -> handlers.running.get() == 4);

        doomed.crash();
        await().atMost(Duration.ofSeconds(15)).until(() -> JDBC.sql(
                "SELECT count(*) FROM jobs WHERE queue = 'rel-crash' AND status = 'RUNNING' AND locked_until < now()")
                .query(Integer.class).single() == 4);

        // The reaper recovers exactly the 4 in-flight jobs and leaves the 6 queued ones alone.
        assertThat(reaper.reapAll()).isEqualTo(new ReapResult(4, 0));
        assertThat(count("rel-crash", "status = 'QUEUED'")).isEqualTo(10);

        // A healthy engine finishes all of them.
        hold.countDown();
        engine("survivor", "rel-crash", 4, true, handlers).start();
        await().atMost(Duration.ofSeconds(20)).until(() -> count("rel-crash", "status = 'SUCCEEDED'") == 10);

        assertThat(count("rel-crash", "attempts = 2")).as("recovered jobs ran twice").isEqualTo(4);
        assertThat(count("rel-crash", "attempts = 1")).as("untouched jobs ran once").isEqualTo(6);
        Integer leaseExpiredRows = JDBC.sql("""
                        SELECT count(*) FROM job_attempts a JOIN jobs j ON j.id = a.job_id
                        WHERE j.queue = 'rel-crash' AND a.outcome = 'LEASE_EXPIRED' AND a.worker_id = 'crashing'
                        """).query(Integer.class).single();
        assertThat(leaseExpiredRows).isEqualTo(4);
    }

    private int count(String queue, String condition) {
        return JDBC.sql("SELECT count(*) FROM jobs WHERE queue = :q AND " + condition)
                .param("q", queue).query(Integer.class).single();
    }
}
