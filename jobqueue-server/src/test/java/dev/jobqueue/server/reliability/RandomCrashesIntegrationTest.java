package dev.jobqueue.server.reliability;

import com.zaxxer.hikari.HikariDataSource;
import dev.jobqueue.common.JobContext;
import dev.jobqueue.common.JobHandler;
import dev.jobqueue.server.PostgresIntegrationTest;
import dev.jobqueue.server.reaper.ReaperProperties;
import dev.jobqueue.server.reaper.ReaperRepository;
import dev.jobqueue.server.reaper.ReaperService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.JsonNode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Randomized, time-boxed reliability test: workers die at random moments while the reaper keeps recovering
 * their jobs. The outcome is not predictable, so the test asserts invariants instead of a sequence of events:
 * no job lost, exactly one SUCCEEDED per job, history consistent.
 *
 * <p>The random seed is printed, so a failure can be investigated with the same sequence of kills (the
 * thread scheduling itself is of course not replayable).
 */
class RandomCrashesIntegrationTest extends PostgresIntegrationTest {

    private static final String QUEUE = "rel-chaos";
    private static final int JOBS = 1_500;
    private static final int WORKERS = 4;
    private static final Duration LEASE = Duration.ofSeconds(3);
    private static final Duration CHAOS_TIME = Duration.ofSeconds(12);

    static class WorkHandlers {
        // job id -> number of times a handler started for it (more than 1 only if a worker died while running it)
        final ConcurrentHashMap<UUID, AtomicInteger> starts = new ConcurrentHashMap<>();

        @JobHandler("work")
        public void work(JobContext context, JsonNode payload) throws InterruptedException {
            starts.computeIfAbsent(context.jobId(), id -> new AtomicInteger()).incrementAndGet();
            Thread.sleep(50); // some work, so that workers are usually busy when they die
        }
    }

    private final WorkHandlers handlers = new WorkHandlers();
    private final List<TestWorkerEngine> engines = new ArrayList<>();
    private HikariDataSource reaperPool;
    private ReaperService reaper;

    @BeforeEach
    void setUp() {
        JDBC.sql("DELETE FROM jobs WHERE status = 'RUNNING'").update();
        reaperPool = newPool(2);
        // A background reaper, like the one every server instance runs.
        reaper = new ReaperService(new ReaperRepository(JdbcClient.create(reaperPool)),
                new ReaperProperties(Duration.ofMillis(200), 100));
    }

    @AfterEach
    void tearDown() {
        reaper.stop();
        engines.forEach(TestWorkerEngine::shutdown);
        reaperPool.close();
    }

    private TestWorkerEngine startEngine(int number) {
        TestWorkerEngine engine = new TestWorkerEngine("chaos-" + number, QUEUE, 2, LEASE, true, newPool(3), handlers);
        engines.add(engine);
        engine.start();
        return engine;
    }

    private int count(String condition) {
        return JDBC.sql("SELECT count(*) FROM jobs WHERE queue = :q AND " + condition)
                .param("q", QUEUE).query(Integer.class).single();
    }

    @Test
    void shouldLoseNoJobAndSucceedEachExactlyOnceWhenWorkersDieAtRandomTimes() throws Exception {
        long seed = System.nanoTime();
        Random random = new Random(seed);

        // max_attempts is generous: every crash costs the jobs it interrupts an attempt.
        JDBC.sql("INSERT INTO jobs (queue, type, max_attempts) SELECT :q, 'work', 30 FROM generate_series(1, :n)")
                .param("q", QUEUE).param("n", JOBS).update();

        reaper.start();
        List<TestWorkerEngine> alive = new ArrayList<>();
        int nextNumber = 1;
        for (int i = 0; i < WORKERS; i++) {
            alive.add(startEngine(nextNumber++));
        }

        // The fault injector: kill a random worker, replace it with a new one (new id), at random intervals.
        // Thread.sleep is fine here: it is the schedule of the injected failures, not a wait for a result.
        long chaosEnds = System.nanoTime() + CHAOS_TIME.toNanos();
        int kills = 0;
        while (System.nanoTime() < chaosEnds && count("status <> 'SUCCEEDED'") > 0) {
            Thread.sleep(100 + random.nextInt(400));
            TestWorkerEngine victim = alive.remove(random.nextInt(alive.size()));
            victim.crash();
            kills++;
            alive.add(startEngine(nextNumber++));
        }

        // Chaos is over; the surviving workers and the reaper must finish everything that is left.
        await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofMillis(250))
                .until(() -> count("status <> 'SUCCEEDED'") == 0);

        // 1. No job lost, none left in flight.
        assertThat(count("status = 'SUCCEEDED'")).as("SUCCEEDED jobs").isEqualTo(JOBS);
        assertThat(count("status IN ('QUEUED', 'RUNNING', 'DEAD')")).as("jobs not finished").isZero();

        // 2. Exactly one SUCCEEDED attempt per job, even though workers died and jobs were retried.
        assertThat(count("""
                (SELECT count(*) FROM job_attempts a WHERE a.job_id = jobs.id AND a.outcome = 'SUCCEEDED') <> 1
                """)).as("jobs without exactly one SUCCEEDED row").isZero();

        // 3. Consistent history: every attempt that was started has exactly one row, so attempts = rows,
        //    and every attempt before the last one was a lease expiry (a crashed worker).
        assertThat(count("attempts <> (SELECT count(*) FROM job_attempts a WHERE a.job_id = jobs.id)"))
                .as("jobs whose attempts counter disagrees with their history").isZero();
        assertThat(count("""
                (SELECT count(*) FROM job_attempts a WHERE a.job_id = jobs.id AND a.outcome = 'LEASE_EXPIRED')
                    <> attempts - 1
                """)).as("jobs with an attempt that is neither the winner nor a lease expiry").isZero();

        // 4. At-least-once: every job's handler ran; more than once only for recovered jobs.
        assertThat(handlers.starts).as("jobs whose handler started").hasSize(JOBS);
        int recovered = count("attempts > 1");
        Integer expiredRows = JDBC.sql("""
                        SELECT count(*) FROM job_attempts a JOIN jobs j ON j.id = a.job_id
                        WHERE j.queue = :q AND a.outcome = 'LEASE_EXPIRED'
                        """).param("q", QUEUE).query(Integer.class).single();
        assertThat(handlers.starts.values().stream().filter(starts -> starts.get() > 1).count())
                .as("jobs that started more than once, but were never recovered")
                .isLessThanOrEqualTo(recovered);

        System.out.println("CHAOS: seed=" + seed + ", workers killed=" + kills + ", jobs recovered=" + recovered
                + ", lease expiries=" + expiredRows + ", jobs=" + JOBS);
        assertThat(kills).as("the test must actually kill workers").isGreaterThan(0);
    }
}
