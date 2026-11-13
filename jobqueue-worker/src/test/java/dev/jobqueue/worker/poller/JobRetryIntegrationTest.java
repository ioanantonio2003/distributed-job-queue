package dev.jobqueue.worker.poller;

import dev.jobqueue.worker.PostgresIntegrationTest;
import dev.jobqueue.worker.WorkerProperties;
import dev.jobqueue.worker.claim.JobClaimRepository;
import dev.jobqueue.worker.completion.JobCompletionRepository;
import dev.jobqueue.worker.demo.FailureDemoHandlers;
import dev.jobqueue.worker.handler.HandlerRegistry;
import dev.jobqueue.worker.heartbeat.RunningJobs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The whole failure path, with the real poller and the demo handlers: retry with backoff, dead-letter,
 * permanent failure.
 */
class JobRetryIntegrationTest extends PostgresIntegrationTest {

    private static final Duration RETRY_BASE = Duration.ofMillis(300);

    private AnnotationConfigApplicationContext spring;
    private JobPoller poller;

    @AfterEach
    void tearDown() {
        if (poller != null) {
            poller.stop();
        }
        if (spring != null) {
            spring.close();
        }
    }

    private void startPoller(String queue) {
        spring = new AnnotationConfigApplicationContext();
        spring.registerBean(ObjectMapper.class, () -> JsonMapper.builder().build());
        spring.register(HandlerRegistry.class, FailureDemoHandlers.class);
        spring.refresh();
        WorkerProperties properties = new WorkerProperties("retry-worker", List.of(queue), 2,
                Duration.ofSeconds(30), Duration.ofMillis(50), Duration.ofMillis(100),
                RETRY_BASE, Duration.ofSeconds(5), null);
        poller = new JobPoller(new JobClaimRepository(JDBC), new JobCompletionRepository(JDBC),
                spring.getBean(HandlerRegistry.class), properties, new RunningJobs());
        poller.start();
    }

    private UUID insertJob(String queue, String type, String payload, int maxAttempts) {
        return JDBC.sql("""
                        INSERT INTO jobs (queue, type, payload, max_attempts)
                        VALUES (:q, :t, CAST(:p AS jsonb), :m) RETURNING id
                        """)
                .param("q", queue).param("t", type).param("p", payload).param("m", maxAttempts)
                .query(UUID.class).single();
    }

    private String status(UUID id) {
        return JDBC.sql("SELECT status FROM jobs WHERE id = :id").param("id", id).query(String.class).single();
    }

    @Test
    void shouldRetryAfterGrowingDelaysAndSucceedWhenHandlerFailsTwice() {
        UUID id = insertJob("retry-flaky", "flaky", "{\"failTimes\":2}", 5);

        startPoller("retry-flaky");

        await().atMost(Duration.ofSeconds(20)).until(() -> status(id).equals("SUCCEEDED"));
        // One row per attempt: FAILED, FAILED, SUCCEEDED; the failures keep their error message.
        var attempts = JDBC.sql("SELECT attempt, outcome, error FROM job_attempts WHERE job_id = :id ORDER BY attempt")
                .param("id", id)
                .query((rs, n) -> rs.getInt("attempt") + ":" + rs.getString("outcome") + ":" + rs.getString("error"))
                .list();
        assertThat(attempts).containsExactly(
                "1:FAILED:IllegalStateException: flaky failure on attempt 1",
                "2:FAILED:IllegalStateException: flaky failure on attempt 2",
                "3:SUCCEEDED:null");
        // Retries wait for at least the jittered backoff (>= half of 300 ms, then >= half of 600 ms).
        var gapsMillis = JDBC.sql("""
                        SELECT (EXTRACT(EPOCH FROM (next_start - finished_at)) * 1000)::bigint AS gap
                        FROM (SELECT finished_at, lead(started_at) OVER (ORDER BY attempt) AS next_start
                              FROM job_attempts WHERE job_id = :id) t
                        WHERE next_start IS NOT NULL ORDER BY next_start
                        """)
                .param("id", id).query(Long.class).list();
        assertThat(gapsMillis).hasSize(2);
        assertThat(gapsMillis.get(0)).isGreaterThanOrEqualTo(RETRY_BASE.toMillis() / 2);
        assertThat(gapsMillis.get(1)).isGreaterThanOrEqualTo(RETRY_BASE.toMillis());
    }

    @Test
    void shouldMarkDeadAfterMaxAttemptsWhenHandlerAlwaysFails() {
        UUID id = insertJob("retry-dead", "always-fail", "{}", 3);

        startPoller("retry-dead");

        await().atMost(Duration.ofSeconds(20)).until(() -> status(id).equals("DEAD"));
        var row = JDBC.sql("SELECT attempts, last_error, finished_at IS NOT NULL AS finished FROM jobs WHERE id = :id")
                .param("id", id)
                .query((rs, n) -> rs.getInt("attempts") + "|" + rs.getString("last_error") + "|" + rs.getBoolean("finished"))
                .single();
        assertThat(row).isEqualTo("3|IllegalStateException: always-fail fails on attempt 3|true");
        Integer failedRows = JDBC.sql("SELECT count(*) FROM job_attempts WHERE job_id = :id AND outcome = 'FAILED'")
                .param("id", id).query(Integer.class).single();
        assertThat(failedRows).isEqualTo(3);
    }

    @Test
    void shouldMarkDeadOnTheFirstAttemptWhenFailureIsPermanent() {
        UUID id = insertJob("retry-permanent", "always-fail", "{\"permanent\":true}", 5);

        startPoller("retry-permanent");

        await().atMost(Duration.ofSeconds(10)).until(() -> status(id).equals("DEAD"));
        Integer attempts = JDBC.sql("SELECT attempts FROM jobs WHERE id = :id").param("id", id)
                .query(Integer.class).single();
        assertThat(attempts).isEqualTo(1);
    }

    @Test
    void shouldMarkDeadAfterMaxAttemptsWhenPayloadCannotBeParsed() {
        // A payload that does not fit the handler's type is a permanent failure: no point retrying it.
        UUID id = insertJob("retry-badpayload", "flaky", "[1, 2]", 5);

        startPoller("retry-badpayload");

        await().atMost(Duration.ofSeconds(10)).until(() -> status(id).equals("DEAD"));
        String error = JDBC.sql("SELECT last_error FROM jobs WHERE id = :id").param("id", id)
                .query(String.class).single();
        assertThat(error).startsWith("PermanentFailureException: Invalid payload for job type 'flaky'");
    }
}
