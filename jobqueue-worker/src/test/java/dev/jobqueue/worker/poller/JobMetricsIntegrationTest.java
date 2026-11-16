package dev.jobqueue.worker.poller;

import dev.jobqueue.common.JobContext;
import dev.jobqueue.common.JobHandler;
import dev.jobqueue.worker.PostgresIntegrationTest;
import dev.jobqueue.worker.WorkerProperties;
import dev.jobqueue.worker.claim.JobClaimRepository;
import dev.jobqueue.worker.completion.JobCompletionRepository;
import dev.jobqueue.worker.demo.FailureDemoHandlers;
import dev.jobqueue.worker.handler.HandlerRegistry;
import dev.jobqueue.worker.heartbeat.RunningJobs;
import dev.jobqueue.worker.metrics.JobMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The worker's metrics: what is counted when a job succeeds, fails, dies, times out or is taken away.
 */
class JobMetricsIntegrationTest extends PostgresIntegrationTest {

    static class MetricHandlers {
        final CountDownLatch blockStarted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        @JobHandler("ok")
        public void ok(JobContext context, JsonNode payload) throws InterruptedException {
            Thread.sleep(30); // measurable, short
        }

        @JobHandler("block")
        public void block(JobContext context, JsonNode payload) throws InterruptedException {
            blockStarted.countDown();
            release.await();
        }
    }

    private final MetricHandlers handlers = new MetricHandlers();
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private AnnotationConfigApplicationContext spring;
    private JobPoller poller;

    @AfterEach
    void tearDown() {
        handlers.release.countDown();
        if (poller != null) {
            poller.stop();
        }
        if (spring != null) {
            spring.close();
        }
    }

    private void start(String queue) {
        spring = new AnnotationConfigApplicationContext();
        spring.registerBean(ObjectMapper.class, () -> JsonMapper.builder().build());
        spring.registerBean("metricHandlers", MetricHandlers.class, () -> handlers);
        spring.register(HandlerRegistry.class, FailureDemoHandlers.class);
        spring.refresh();
        WorkerProperties properties = new WorkerProperties("metrics-worker", List.of(queue), 2,
                Duration.ofSeconds(3), Duration.ofMillis(50), Duration.ofMillis(100), Duration.ofMillis(100),
                Duration.ofSeconds(1), null, null, Duration.ofSeconds(1));
        poller = new JobPoller(new JobClaimRepository(JDBC), new JobCompletionRepository(JDBC),
                spring.getBean(HandlerRegistry.class), properties, new RunningJobs(), new JobMetrics(meters));
        poller.start();
    }

    private void insertJob(String queue, String type, int maxAttempts, int timeoutSeconds) {
        JDBC.sql("INSERT INTO jobs (queue, type, max_attempts, timeout_seconds) VALUES (:q, :t, :m, :s)")
                .param("q", queue).param("t", type).param("m", maxAttempts).param("s", timeoutSeconds).update();
    }

    private double processed(String queue, String outcome) {
        Counter counter = meters.find("jobqueue.jobs.processed").tag("queue", queue).tag("outcome", outcome).counter();
        return counter == null ? 0 : counter.count();
    }

    private double dead(String queue) {
        Counter counter = meters.find("jobqueue.jobs.dead").tag("queue", queue).counter();
        return counter == null ? 0 : counter.count();
    }

    private long handlerRuns(String queue, String type, String outcome) {
        Timer timer = meters.find("jobqueue.handler.duration")
                .tag("queue", queue).tag("type", type).tag("outcome", outcome).timer();
        return timer == null ? 0 : timer.count();
    }

    @Test
    void shouldCountASuccessAndTimeTheHandlerAndTheWaitInTheQueue() {
        insertJob("met-ok", "ok", 5, 60);

        start("met-ok");

        await().atMost(Duration.ofSeconds(10)).until(() -> processed("met-ok", "succeeded") == 1);
        assertThat(handlerRuns("met-ok", "ok", "success")).isEqualTo(1);
        Timer handler = meters.get("jobqueue.handler.duration").tag("type", "ok").timer();
        assertThat(handler.totalTime(TimeUnit.MILLISECONDS)).as("the 30 ms sleep was measured").isGreaterThanOrEqualTo(30);
        Timer wait = meters.get("jobqueue.job.wait").tag("queue", "met-ok").timer();
        assertThat(wait.count()).isEqualTo(1);
        assertThat(wait.totalTime(TimeUnit.MILLISECONDS)).isGreaterThanOrEqualTo(0);
        assertThat(dead("met-ok")).isZero();
    }

    @Test
    void shouldCountEveryFailedAttemptAndTheDeathOfTheJobThatRunsOutOfAttempts() {
        insertJob("met-fail", "always-fail", 2, 60); // 2 attempts, both fail

        start("met-fail");

        await().atMost(Duration.ofSeconds(15)).until(() -> dead("met-fail") == 1);
        assertThat(processed("met-fail", "failed")).isEqualTo(2);
        assertThat(handlerRuns("met-fail", "always-fail", "error")).isEqualTo(2);
        assertThat(processed("met-fail", "succeeded")).isZero();
    }

    @Test
    void shouldCountATimeoutSeparatelyFromAFailure() {
        insertJob("met-timeout", "hang", 1, 1); // 1 s limit, 1 attempt

        start("met-timeout");

        await().atMost(Duration.ofSeconds(15)).until(() -> dead("met-timeout") == 1);
        assertThat(processed("met-timeout", "timed_out")).isEqualTo(1);
        assertThat(processed("met-timeout", "failed")).isZero();
    }

    @Test
    void shouldCountALostLeaseWhenTheResultOfAJobTakenAwayIsIgnored() throws Exception {
        insertJob("met-lost", "block", 5, 60);
        start("met-lost");
        assertThat(handlers.blockStarted.await(10, TimeUnit.SECONDS)).isTrue();

        // Another worker takes the job over while the handler is still running.
        JDBC.sql("UPDATE jobs SET locked_by = 'thief', attempts = attempts + 1 WHERE queue = 'met-lost'").update();
        handlers.release.countDown();

        await().atMost(Duration.ofSeconds(10)).until(() -> processed("met-lost", "lease_lost") == 1);
        assertThat(processed("met-lost", "succeeded")).isZero();
    }
}
