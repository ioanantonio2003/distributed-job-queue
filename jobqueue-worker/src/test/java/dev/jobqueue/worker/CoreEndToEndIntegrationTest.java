package dev.jobqueue.worker;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.jobqueue.common.JobContext;
import dev.jobqueue.common.JobHandler;
import dev.jobqueue.worker.claim.JobClaimRepository;
import dev.jobqueue.worker.completion.JobCompletionRepository;
import dev.jobqueue.worker.handler.HandlerRegistry;
import dev.jobqueue.worker.poller.JobPoller;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end proof of the core: several worker engines competing for the same jobs run every job exactly once.
 *
 * <p>The engines live in one JVM but share nothing except the database, which is the only thing that
 * coordinates real workers on different machines too. What this cannot show is process death or network
 * failure; the chaos tests cover that later.
 */
class CoreEndToEndIntegrationTest extends PostgresIntegrationTest {

    private static final String QUEUE = "e2e-core";
    private static final int WORKERS = 4;
    private static final int SLOTS_PER_WORKER = 8;
    private static final int JOBS = 5_000;

    static class CountingHandler {
        // job id -> how many times its handler ran; anything other than 1 is a bug
        final ConcurrentHashMap<UUID, AtomicInteger> runs = new ConcurrentHashMap<>();

        @JobHandler("count")
        public void count(JobContext context, JsonNode payload) {
            runs.computeIfAbsent(context.jobId(), id -> new AtomicInteger()).incrementAndGet();
        }
    }

    private final CountingHandler handler = new CountingHandler();
    private final List<JobPoller> pollers = new ArrayList<>();
    private AnnotationConfigApplicationContext spring;
    private HikariDataSource pool;

    @AfterEach
    void tearDown() {
        pollers.forEach(JobPoller::stop);
        if (spring != null) {
            spring.close();
        }
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    void shouldRunEveryJobExactlyOnceWhenFourWorkersCompeteForFiveThousandJobs() {
        // A real connection pool, like the workers have in production (the base class's data source opens
        // a new connection for every statement, which would only slow this test down).
        var plain = (DriverManagerDataSource) DATA_SOURCE;
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(plain.getUrl());
        config.setUsername(plain.getUsername());
        config.setPassword(plain.getPassword());
        config.setMaximumPoolSize(16);
        pool = new HikariDataSource(config);
        JdbcClient jdbc = JdbcClient.create(pool);

        spring = new AnnotationConfigApplicationContext();
        spring.registerBean(ObjectMapper.class, () -> JsonMapper.builder().build());
        spring.registerBean("countingHandler", CountingHandler.class, () -> handler);
        spring.register(HandlerRegistry.class);
        spring.refresh();
        HandlerRegistry registry = spring.getBean(HandlerRegistry.class);

        jdbc.sql("INSERT INTO jobs (queue, type) SELECT :q, 'count' FROM generate_series(1, :n)")
                .param("q", QUEUE).param("n", JOBS).update();

        long startNanos = System.nanoTime();
        for (int i = 1; i <= WORKERS; i++) {
            WorkerProperties properties = new WorkerProperties("e2e-worker-" + i, List.of(QUEUE), SLOTS_PER_WORKER,
                    Duration.ofSeconds(60), Duration.ofMillis(20), Duration.ofMillis(100));
            JobPoller poller = new JobPoller(new JobClaimRepository(jdbc), new JobCompletionRepository(jdbc),
                    registry, properties);
            pollers.add(poller);
            poller.start();
        }

        await().atMost(Duration.ofSeconds(120)).pollInterval(Duration.ofMillis(200))
                .until(() -> countJobs(jdbc, "status = 'SUCCEEDED'") == JOBS);
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - startNanos).toMillis();

        // 1. Every job's handler ran exactly once.
        assertThat(handler.runs).as("distinct jobs executed").hasSize(JOBS);
        assertThat(handler.runs.values()).as("handler runs per job").allMatch(runs -> runs.get() == 1);

        // 2. Nothing is left in flight, and nothing needed a second attempt.
        assertThat(countJobs(jdbc, "status = 'RUNNING'")).isZero();
        assertThat(countJobs(jdbc, "status <> 'SUCCEEDED'")).isZero();
        assertThat(countJobs(jdbc, "attempts <> 1")).isZero();

        // 3. Exactly one SUCCEEDED attempt row per job.
        assertThat(jdbc.sql("""
                        SELECT count(*) FROM job_attempts a JOIN jobs j ON j.id = a.job_id
                        WHERE j.queue = :q AND a.outcome = 'SUCCEEDED'
                        """).param("q", QUEUE).query(Integer.class).single()).isEqualTo(JOBS);
        assertThat(countJobs(jdbc, """
                        (SELECT count(*) FROM job_attempts a WHERE a.job_id = jobs.id AND a.outcome = 'SUCCEEDED') <> 1
                        """)).isZero();

        // Informational only (not asserted): how the work was spread over the workers.
        String distribution = jdbc.sql("""
                        SELECT a.worker_id || '=' || count(*) FROM job_attempts a JOIN jobs j ON j.id = a.job_id
                        WHERE j.queue = :q GROUP BY a.worker_id ORDER BY a.worker_id
                        """).param("q", QUEUE).query(String.class).list().toString();
        System.out.println("E2E: " + JOBS + " jobs, " + WORKERS + " workers, " + elapsedMillis + " ms, "
                + distribution);
    }

    private static int countJobs(JdbcClient jdbc, String condition) {
        return jdbc.sql("SELECT count(*) FROM jobs WHERE queue = :q AND " + condition)
                .param("q", QUEUE).query(Integer.class).single();
    }
}
