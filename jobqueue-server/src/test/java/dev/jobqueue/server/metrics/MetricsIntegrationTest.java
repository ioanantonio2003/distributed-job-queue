package dev.jobqueue.server.metrics;

import dev.jobqueue.server.PostgresIntegrationTest;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The server's metrics: the enqueue counter, the queue gauges refreshed from the database, and the
 * Prometheus text endpoint.
 */
@SpringBootTest(properties = "jobqueue.reaper.enabled=false")
@AutoConfigureMockMvc
class MetricsIntegrationTest extends PostgresIntegrationTest {

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    MockMvc mvc;
    @Autowired
    MeterRegistry registry;
    @Autowired
    QueueMetrics queueMetrics;

    private double enqueued(String queue) {
        Counter counter = registry.find("jobqueue.jobs.enqueued").tag("queue", queue).counter();
        return counter == null ? 0 : counter.count();
    }

    private double gauge(String name, String... tags) {
        return registry.get(name).tags(tags).gauge().value();
    }

    @Test
    void shouldCountEveryEnqueuedJobPerQueueAndNotARepeatedIdempotentRequest() throws Exception {
        String single = "{\"queue\":\"met-enq\",\"type\":\"t\",\"idempotencyKey\":\"same\"}";

        mvc.perform(post("/jobs").contentType(MediaType.APPLICATION_JSON).content(single))
                .andExpect(status().isCreated());
        assertThat(enqueued("met-enq")).isEqualTo(1);

        mvc.perform(post("/jobs").contentType(MediaType.APPLICATION_JSON).content(single))
                .andExpect(status().isOk()); // the same job again: not a new one
        assertThat(enqueued("met-enq")).isEqualTo(1);

        mvc.perform(post("/jobs/batch").contentType(MediaType.APPLICATION_JSON).content("""
                        {"jobs":[{"queue":"met-enq","type":"t"},{"queue":"met-enq","type":"t"},
                                 {"queue":"met-enq-b","type":"t"}]}
                        """))
                .andExpect(status().isCreated());
        assertThat(enqueued("met-enq")).isEqualTo(3);
        assertThat(enqueued("met-enq-b")).isEqualTo(1);
    }

    @Test
    void shouldPublishQueueDepthAndOldestWaitFromTheDatabase() {
        JDBC.sql("INSERT INTO jobs (queue, type, run_at) VALUES ('met-depth', 't', now() - interval '90 seconds')").update();
        JDBC.sql("INSERT INTO jobs (queue, type) VALUES ('met-depth', 't')").update();
        JDBC.sql("INSERT INTO jobs (queue, type, run_at) VALUES ('met-depth', 't', now() + interval '1 hour')").update();
        JDBC.sql("INSERT INTO jobs (queue, type, status) VALUES ('met-depth', 't', 'RUNNING')").update();
        JDBC.sql("INSERT INTO jobs (queue, type, status) VALUES ('met-depth', 't', 'SUCCEEDED')").update(); // not counted

        queueMetrics.refresh();

        assertThat(gauge("jobqueue.queue.jobs", "queue", "met-depth", "state", "ready")).isEqualTo(2);
        assertThat(gauge("jobqueue.queue.jobs", "queue", "met-depth", "state", "scheduled")).isEqualTo(1);
        assertThat(gauge("jobqueue.queue.jobs", "queue", "met-depth", "state", "running")).isEqualTo(1);
        // The oldest ready job became runnable 90 s ago; the job scheduled for later does not count as waiting.
        assertThat(gauge("jobqueue.queue.oldest.age.seconds", "queue", "met-depth")).isBetween(90.0, 150.0);

        // When the queue empties its gauges go to 0 instead of disappearing, so graphs show 0, not a gap.
        JDBC.sql("DELETE FROM jobs WHERE queue = 'met-depth'").update();
        queueMetrics.refresh();
        assertThat(gauge("jobqueue.queue.jobs", "queue", "met-depth", "state", "ready")).isZero();
        assertThat(gauge("jobqueue.queue.jobs", "queue", "met-depth", "state", "running")).isZero();
        assertThat(gauge("jobqueue.queue.oldest.age.seconds", "queue", "met-depth")).isZero();
    }

    @Test
    void shouldServeTheMetricsInPrometheusFormat() throws Exception {
        mvc.perform(post("/jobs").contentType(MediaType.APPLICATION_JSON)
                .content("{\"queue\":\"met-prom\",\"type\":\"t\"}"));
        queueMetrics.refresh();

        String text = mvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        assertThat(text)
                .contains("jobqueue_jobs_enqueued_total{queue=\"met-prom\"}")
                .contains("jobqueue_queue_jobs{queue=\"met-prom\",state=\"ready\"}")
                .contains("jobqueue_queue_oldest_age_seconds{queue=\"met-prom\"}");
    }
}
