package dev.jobqueue.server.job;

import com.jayway.jsonpath.JsonPath;
import dev.jobqueue.server.PostgresIntegrationTest;
import dev.jobqueue.worker.claim.ClaimedJob;
import dev.jobqueue.worker.claim.JobClaimRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Delayed jobs and priorities, from the HTTP request down to the worker's real claim statement.
 * Times are compared on the database clock (now() in SQL), the same clock the system itself uses.
 */
@SpringBootTest(properties = "jobqueue.reaper.enabled=false")
@AutoConfigureMockMvc
class JobSchedulingIntegrationTest extends PostgresIntegrationTest {

    private static final Duration LEASE = Duration.ofSeconds(30);

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    MockMvc mvc;

    private final JobClaimRepository claimRepository = new JobClaimRepository(JDBC);

    private ResultActions send(String json) throws Exception {
        return mvc.perform(post("/jobs").contentType(MediaType.APPLICATION_JSON).content(json));
    }

    private UUID postJob(String json) throws Exception {
        String body = send(json).andExpect(status().isCreated()).andReturn().getResponse().getContentAsString();
        return UUID.fromString(JsonPath.read(body, "$.id"));
    }

    private boolean runAtWithin(UUID id, String lowerExpr, String upperExpr) {
        return JDBC.sql("SELECT run_at > " + lowerExpr + " AND run_at <= " + upperExpr + " FROM jobs WHERE id = :id")
                .param("id", id).query(Boolean.class).single();
    }

    private Instant databaseNowPlus(String interval) {
        return JDBC.sql("SELECT now() + CAST(:i AS interval)").param("i", interval)
                .query(OffsetDateTime.class).single().toInstant();
    }

    // ---- delay --------------------------------------------------------------------------------------------

    @Test
    void shouldScheduleOnTheDatabaseClockWhenDelaySecondsIsGiven() throws Exception {
        UUID id = postJob("{\"queue\":\"sched-delay\",\"type\":\"t\",\"delaySeconds\":3600}");

        // run_at is "database now + 1 hour" (taken at insert time, so slightly before the check's own now()).
        assertThat(runAtWithin(id, "now() + interval '3590 seconds'", "now() + interval '3600 seconds'")).isTrue();
    }

    @Test
    void shouldStartNowWhenNoDelayIsGiven() throws Exception {
        UUID id = postJob("{\"queue\":\"sched-now\",\"type\":\"t\"}");

        assertThat(runAtWithin(id, "now() - interval '10 seconds'", "now()")).isTrue();
    }

    @Test
    void shouldStoreTheGivenInstantWhenRunAtIsGiven() throws Exception {
        Instant runAt = databaseNowPlus("1 hour");

        UUID id = postJob("{\"queue\":\"sched-runat\",\"type\":\"t\",\"runAt\":\"" + runAt + "\"}");

        Boolean same = JDBC.sql("SELECT run_at = :t FROM jobs WHERE id = :id")
                .param("t", runAt.atOffset(ZoneOffset.UTC)).param("id", id).query(Boolean.class).single();
        assertThat(same).isTrue();
    }

    @Test
    void shouldTreatAPastRunAtAsStartNow() throws Exception {
        Instant yesterday = databaseNowPlus("-1 day");

        UUID id = postJob("{\"queue\":\"sched-past\",\"type\":\"t\",\"runAt\":\"" + yesterday + "\"}");

        assertThat(runAtWithin(id, "now() - interval '2 days'", "now()")).isTrue();
    }

    @Test
    void shouldRejectTheRequestWhenBothDelaySecondsAndRunAtAreGiven() throws Exception {
        send("{\"queue\":\"q\",\"type\":\"t\",\"delaySeconds\":10,\"runAt\":\"" + databaseNowPlus("1 hour") + "\"}")
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value("give either delaySeconds or runAt, not both"));
    }

    @Test
    void shouldRejectNegativeAndTooLargeDelays() throws Exception {
        send("{\"queue\":\"q\",\"type\":\"t\",\"delaySeconds\":-1}")
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.errors.delaySeconds").exists());
        send("{\"queue\":\"q\",\"type\":\"t\",\"delaySeconds\":2592001}") // 30 days + 1 second
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.errors.delaySeconds").exists());
        send("{\"queue\":\"sched-limit\",\"type\":\"t\",\"delaySeconds\":2592000}")
                .andExpect(status().isCreated());
    }

    @Test
    void shouldRejectARunAtMoreThanThirtyDaysAhead() throws Exception {
        send("{\"queue\":\"q\",\"type\":\"t\",\"runAt\":\"" + databaseNowPlus("31 days") + "\"}")
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value(containsString("30 days")));
    }

    // ---- priority ------------------------------------------------------------------------------------------

    @Test
    void shouldAcceptPriorityBetweenMinusHundredAndHundredOnly() throws Exception {
        send("{\"queue\":\"q\",\"type\":\"t\",\"priority\":101}")
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.errors.priority").exists());
        send("{\"queue\":\"q\",\"type\":\"t\",\"priority\":-101}")
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.errors.priority").exists());
        send("{\"queue\":\"sched-prio-edge\",\"type\":\"t\",\"priority\":100}").andExpect(status().isCreated());
        send("{\"queue\":\"sched-prio-edge\",\"type\":\"t\",\"priority\":-100}").andExpect(status().isCreated());
    }

    // ---- end to end with the real claim statement ----------------------------------------------------------

    private List<String> claimOneAtATime(String queue, int times) {
        List<String> types = new ArrayList<>();
        for (int i = 0; i < times; i++) {
            claimRepository.claim("sched-worker", queue, LEASE, 1).forEach(job -> types.add(job.type()));
        }
        return types;
    }

    @Test
    void shouldClaimHigherPriorityJobsFirstWhenTheyAreEnqueuedThroughTheApi() throws Exception {
        postJob("{\"queue\":\"sched-prio\",\"type\":\"low\",\"priority\":0}");
        postJob("{\"queue\":\"sched-prio\",\"type\":\"high\",\"priority\":9}");
        postJob("{\"queue\":\"sched-prio\",\"type\":\"mid\",\"priority\":5}");
        postJob("{\"queue\":\"sched-prio\",\"type\":\"urgent\",\"priority\":100}");
        postJob("{\"queue\":\"sched-prio\",\"type\":\"lazy\",\"priority\":-100}");

        assertThat(claimOneAtATime("sched-prio", 5)).containsExactly("urgent", "high", "mid", "low", "lazy");
    }

    @Test
    void shouldClaimTheEarliestRunAtFirstWhenPrioritiesAreEqual() throws Exception {
        postJob("{\"queue\":\"sched-tie\",\"type\":\"newest\",\"priority\":3}"); // run_at = now
        postJob("{\"queue\":\"sched-tie\",\"type\":\"oldest\",\"priority\":3,\"runAt\":\"" + databaseNowPlus("-2 hours") + "\"}");
        postJob("{\"queue\":\"sched-tie\",\"type\":\"older\",\"priority\":3,\"runAt\":\"" + databaseNowPlus("-1 hour") + "\"}");

        assertThat(claimOneAtATime("sched-tie", 3)).containsExactly("oldest", "older", "newest");
    }

    @Test
    void shouldNotClaimADelayedJobBeforeItsTimeAndClaimItAfterwards() throws Exception {
        UUID id = postJob("{\"queue\":\"sched-wait\",\"type\":\"later\",\"delaySeconds\":2}");

        assertThat(claimRepository.claim("sched-worker", "sched-wait", LEASE, 10))
                .as("claimed before its time").isEmpty();

        AtomicReference<List<ClaimedJob>> claimed = new AtomicReference<>(List.of());
        await().atMost(Duration.ofSeconds(10)).until(() -> {
            claimed.set(claimRepository.claim("sched-worker", "sched-wait", LEASE, 10));
            return !claimed.get().isEmpty();
        });
        assertThat(claimed.get()).singleElement().satisfies(job -> assertThat(job.id()).isEqualTo(id));
    }

    @Test
    void shouldLetAnUrgentJobOvertakeAnEarlierOneButNotADelayedOne() throws Exception {
        postJob("{\"queue\":\"sched-mix\",\"type\":\"normal\"}");
        postJob("{\"queue\":\"sched-mix\",\"type\":\"urgent-but-later\",\"priority\":50,\"delaySeconds\":3600}");
        postJob("{\"queue\":\"sched-mix\",\"type\":\"urgent\",\"priority\":50}");

        // urgent overtakes normal; urgent-but-later is not runnable yet, however high its priority.
        assertThat(claimOneAtATime("sched-mix", 3)).containsExactly("urgent", "normal");
    }

    // ---- batch ---------------------------------------------------------------------------------------------

    @Test
    void shouldApplyDelayAndRunAtToEachJobWhenEnqueuingABatch() throws Exception {
        String body = "{\"jobs\":["
                + "{\"queue\":\"sched-batch\",\"type\":\"delayed\",\"delaySeconds\":3600,\"priority\":7},"
                + "{\"queue\":\"sched-batch\",\"type\":\"absolute\",\"runAt\":\"" + databaseNowPlus("2 hours") + "\"},"
                + "{\"queue\":\"sched-batch\",\"type\":\"now\"}]}";

        String response = mvc.perform(post("/jobs/batch").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString();

        UUID delayed = UUID.fromString(JsonPath.read(response, "$.ids[0]"));
        UUID absolute = UUID.fromString(JsonPath.read(response, "$.ids[1]"));
        UUID immediate = UUID.fromString(JsonPath.read(response, "$.ids[2]"));
        assertThat(runAtWithin(delayed, "now() + interval '3590 seconds'", "now() + interval '3600 seconds'")).isTrue();
        assertThat(runAtWithin(absolute, "now() + interval '7190 seconds'", "now() + interval '7200 seconds'")).isTrue();
        assertThat(runAtWithin(immediate, "now() - interval '10 seconds'", "now()")).isTrue();
    }

    @Test
    void shouldStoreNothingWhenAJobInTheBatchHasBothDelayAndRunAt() throws Exception {
        String body = "{\"jobs\":["
                + "{\"queue\":\"sched-batch-bad\",\"type\":\"ok\"},"
                + "{\"queue\":\"sched-batch-bad\",\"type\":\"bad\",\"delaySeconds\":5,\"runAt\":\"" + databaseNowPlus("1 hour") + "\"}]}";

        mvc.perform(post("/jobs/batch").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest());

        Integer rows = JDBC.sql("SELECT count(*) FROM jobs WHERE queue = 'sched-batch-bad'")
                .query(Integer.class).single();
        assertThat(rows).isZero();
    }
}
