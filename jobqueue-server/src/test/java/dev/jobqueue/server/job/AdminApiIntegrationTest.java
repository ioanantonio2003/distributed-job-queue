package dev.jobqueue.server.job;

import com.jayway.jsonpath.JsonPath;
import dev.jobqueue.server.PostgresIntegrationTest;
import dev.jobqueue.worker.claim.ClaimedJob;
import dev.jobqueue.worker.claim.JobClaimRepository;
import dev.jobqueue.worker.completion.JobCompletionRepository;
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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The admin endpoints: listing, detail with history, cancel, retry, queue overview, and the OpenAPI description.
 */
@SpringBootTest(properties = "jobqueue.reaper.enabled=false")
@AutoConfigureMockMvc
class AdminApiIntegrationTest extends PostgresIntegrationTest {

    private static final Duration LEASE = Duration.ofSeconds(30);

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    MockMvc mvc;
    @Autowired
    JobService jobService;

    private final JobClaimRepository claimRepository = new JobClaimRepository(JDBC);
    private final JobCompletionRepository completionRepository = new JobCompletionRepository(JDBC);

    /** Inserts a job directly in the given status (the API cannot create jobs in arbitrary states). */
    private UUID insertJob(String queue, String type, String status) {
        return JDBC.sql("INSERT INTO jobs (queue, type, status) VALUES (:q, :t, :s) RETURNING id")
                .param("q", queue).param("t", type).param("s", status).query(UUID.class).single();
    }

    private void insertAttempt(UUID jobId, int attempt, String outcome) {
        JDBC.sql("""
                        INSERT INTO job_attempts (job_id, attempt, worker_id, started_at, finished_at, outcome, error)
                        VALUES (:id, :a, 'worker-x', now() - interval '1 minute', now(), :o, :e)
                        """)
                .param("id", jobId).param("a", attempt).param("o", outcome)
                .param("e", "FAILED".equals(outcome) ? "boom" : null).update();
    }

    private String body(ResultActions actions) throws Exception {
        return actions.andReturn().getResponse().getContentAsString();
    }

    // ---- listing -------------------------------------------------------------------------------------------

    @Test
    void shouldFilterJobsByQueueStatusAndTypeAndReportTheTotal() throws Exception {
        for (int i = 0; i < 3; i++) {
            insertJob("adm-list", "alpha", "QUEUED");
        }
        for (int i = 0; i < 2; i++) {
            insertJob("adm-list", "beta", "SUCCEEDED");
        }
        insertJob("adm-list-other", "alpha", "QUEUED");

        mvc.perform(get("/jobs").param("queue", "adm-list"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(5))
                .andExpect(jsonPath("$.items.length()").value(5));
        mvc.perform(get("/jobs").param("queue", "adm-list").param("status", "QUEUED"))
                .andExpect(jsonPath("$.total").value(3));
        mvc.perform(get("/jobs").param("queue", "adm-list").param("type", "beta"))
                .andExpect(jsonPath("$.total").value(2));
        mvc.perform(get("/jobs").param("queue", "adm-list").param("status", "SUCCEEDED").param("type", "alpha"))
                .andExpect(jsonPath("$.total").value(0))
                .andExpect(jsonPath("$.items.length()").value(0));
    }

    @Test
    void shouldPageThroughJobsNewestFirstWhenListing() throws Exception {
        List<UUID> inserted = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            inserted.add(insertJob("adm-page", "t", "QUEUED")); // separate statements: increasing created_at
        }

        String first = body(mvc.perform(get("/jobs").param("queue", "adm-page").param("size", "10")));
        assertThat((Integer) JsonPath.read(first, "$.total")).isEqualTo(25);
        assertThat((Integer) JsonPath.read(first, "$.items.length()")).isEqualTo(10);
        // Newest first: the last one inserted leads the first page.
        assertThat((String) JsonPath.read(first, "$.items[0].id")).isEqualTo(inserted.get(24).toString());

        mvc.perform(get("/jobs").param("queue", "adm-page").param("size", "10").param("page", "2"))
                .andExpect(jsonPath("$.items.length()").value(5))
                .andExpect(jsonPath("$.page").value(2))
                .andExpect(jsonPath("$.items[4].id").value(inserted.get(0).toString())); // oldest is last
        mvc.perform(get("/jobs").param("queue", "adm-page").param("size", "10").param("page", "3"))
                .andExpect(jsonPath("$.items.length()").value(0))
                .andExpect(jsonPath("$.total").value(25));
    }

    @Test
    void shouldRejectOutOfRangePagingAndUnknownStatusWhenListing() throws Exception {
        mvc.perform(get("/jobs").param("size", "101"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
        mvc.perform(get("/jobs").param("size", "0")).andExpect(status().isBadRequest());
        mvc.perform(get("/jobs").param("page", "-1")).andExpect(status().isBadRequest());
        mvc.perform(get("/jobs").param("status", "NOPE")).andExpect(status().isBadRequest());
        mvc.perform(get("/jobs").param("size", "100")).andExpect(status().isOk()); // the limit itself is allowed
    }

    // ---- detail --------------------------------------------------------------------------------------------

    @Test
    void shouldReturnTheJobWithItsHistoryWhenGettingById() throws Exception {
        UUID id = insertJob("adm-detail", "t", "DEAD");
        insertAttempt(id, 1, "FAILED");
        insertAttempt(id, 2, "LEASE_EXPIRED");

        mvc.perform(get("/jobs/{id}", id))
                .andExpect(status().isOk())
                // the job's own fields are still at the top level...
                .andExpect(jsonPath("$.id").value(id.toString()))
                .andExpect(jsonPath("$.status").value("DEAD"))
                // ...and the history comes next to them, in attempt order
                .andExpect(jsonPath("$.history.length()").value(2))
                .andExpect(jsonPath("$.history[0].attempt").value(1))
                .andExpect(jsonPath("$.history[0].outcome").value("FAILED"))
                .andExpect(jsonPath("$.history[0].error").value("boom"))
                .andExpect(jsonPath("$.history[0].workerId").value("worker-x"))
                .andExpect(jsonPath("$.history[1].outcome").value("LEASE_EXPIRED"));
    }

    @Test
    void shouldReturnAnEmptyHistoryWhenTheJobHasNotRunYet() throws Exception {
        UUID id = insertJob("adm-detail", "t", "QUEUED");

        mvc.perform(get("/jobs/{id}", id)).andExpect(jsonPath("$.history.length()").value(0));
    }

    // ---- cancel --------------------------------------------------------------------------------------------

    @Test
    void shouldCancelAQueuedJobAndNeverLetAWorkerClaimIt() throws Exception {
        UUID id = insertJob("adm-cancel-a", "t", "QUEUED");

        mvc.perform(post("/jobs/{id}/cancel", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"))
                .andExpect(jsonPath("$.finishedAt").isNotEmpty());

        assertThat(claimRepository.claim("w", "adm-cancel-a", LEASE, 10)).isEmpty();
    }

    @Test
    void shouldAnswerConflictWhenCancellingAJobThatIsRunningOrFinished() throws Exception {
        UUID running = insertJob("adm-cancel-b", "t", "RUNNING");
        UUID succeeded = insertJob("adm-cancel-b", "t", "SUCCEEDED");
        UUID cancelled = insertJob("adm-cancel-b", "t", "CANCELLED");

        mvc.perform(post("/jobs/{id}/cancel", running))
                .andExpect(status().isConflict())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.detail").value(containsString("RUNNING")))
                .andExpect(jsonPath("$.detail").value(containsString("cannot be stopped from outside")));
        mvc.perform(post("/jobs/{id}/cancel", succeeded)).andExpect(status().isConflict());
        mvc.perform(post("/jobs/{id}/cancel", cancelled)).andExpect(status().isConflict());
        // nothing changed
        assertThat(JDBC.sql("SELECT status FROM jobs WHERE id = :id").param("id", running).query(String.class).single())
                .isEqualTo("RUNNING");
    }

    @Test
    void shouldAnswerNotFoundWhenCancellingOrRetryingAnUnknownJob() throws Exception {
        mvc.perform(post("/jobs/{id}/cancel", UUID.randomUUID())).andExpect(status().isNotFound());
        mvc.perform(post("/jobs/{id}/retry", UUID.randomUUID())).andExpect(status().isNotFound());
    }

    @Test
    void shouldEndEveryJobEitherCancelledOrClaimedNeverBothWhenCancellingWhileWorkersClaim() throws Exception {
        int jobs = 300;
        JDBC.sql("INSERT INTO jobs (queue, type) SELECT 'adm-race', 't' FROM generate_series(1, :n)")
                .param("n", jobs).update();
        List<UUID> ids = JDBC.sql("SELECT id FROM jobs WHERE queue = 'adm-race'").query(UUID.class).list();

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<?> canceller = pool.submit(() -> {
            start.await();
            for (UUID id : ids) {
                try {
                    jobService.cancel(id);
                } catch (InvalidJobStateException alreadyClaimed) {
                    // a worker won this one: fine
                }
            }
            return null;
        });
        Future<?> claimer = pool.submit(() -> {
            start.await();
            List<ClaimedJob> batch;
            do {
                batch = claimRepository.claim("race-worker", "adm-race", LEASE, 5);
            } while (!batch.isEmpty());
            return null;
        });
        start.countDown();
        canceller.get();
        claimer.get();
        pool.shutdown();

        // Every job went through exactly one of the two transitions.
        Integer cancelled = JDBC.sql("SELECT count(*) FROM jobs WHERE queue = 'adm-race' AND status = 'CANCELLED'")
                .query(Integer.class).single();
        Integer running = JDBC.sql("SELECT count(*) FROM jobs WHERE queue = 'adm-race' AND status = 'RUNNING'")
                .query(Integer.class).single();
        Integer claimedButCancelled = JDBC.sql(
                "SELECT count(*) FROM jobs WHERE queue = 'adm-race' AND status = 'CANCELLED' AND attempts > 0")
                .query(Integer.class).single();
        assertThat(cancelled + running).isEqualTo(jobs);
        assertThat(claimedButCancelled).as("jobs both claimed and cancelled").isZero();
    }

    // ---- retry ---------------------------------------------------------------------------------------------

    @Test
    void shouldPutADeadJobBackAndLetItRunAgainWithoutBreakingItsHistory() throws Exception {
        UUID id = JDBC.sql("""
                        INSERT INTO jobs (queue, type, status, attempts, max_attempts, finished_at, last_error)
                        VALUES ('adm-retry-a', 't', 'DEAD', 3, 3, now(), 'boom') RETURNING id
                        """).query(UUID.class).single();
        for (int attempt = 1; attempt <= 3; attempt++) {
            insertAttempt(id, attempt, "FAILED");
        }

        mvc.perform(post("/jobs/{id}/retry", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("QUEUED"))
                .andExpect(jsonPath("$.maxAttempts").value(6)) // a fresh set of 3 more attempts
                .andExpect(jsonPath("$.attempts").value(3))    // numbering goes on, history stays valid
                .andExpect(jsonPath("$.finishedAt").isEmpty());

        // A worker claims it as attempt 4 and completes it: writing history row 4 does not collide with 1..3.
        List<ClaimedJob> claimed = claimRepository.claim("retry-worker", "adm-retry-a", LEASE, 1);
        assertThat(claimed).singleElement().satisfies(job -> assertThat(job.attempt()).isEqualTo(4));
        assertThat(completionRepository.complete(id, "retry-worker", 4)).isTrue();
        mvc.perform(get("/jobs/{id}", id))
                .andExpect(jsonPath("$.status").value("SUCCEEDED"))
                .andExpect(jsonPath("$.history.length()").value(4))
                .andExpect(jsonPath("$.history[3].outcome").value("SUCCEEDED"));
    }

    @Test
    void shouldGiveAFullNewBudgetWhenRetryingAJobThatDiedOnItsFirstAttempt() throws Exception {
        // A permanent failure: DEAD after 1 of 5 attempts.
        UUID id = JDBC.sql("""
                        INSERT INTO jobs (queue, type, status, attempts, max_attempts, finished_at)
                        VALUES ('adm-retry-b', 't', 'DEAD', 1, 5, now()) RETURNING id
                        """).query(UUID.class).single();
        insertAttempt(id, 1, "FAILED");

        mvc.perform(post("/jobs/{id}/retry", id))
                .andExpect(jsonPath("$.maxAttempts").value(6)) // 5 more attempts after the first
                .andExpect(jsonPath("$.attempts").value(1));
    }

    @Test
    void shouldAnswerConflictWhenRetryingAJobThatIsNotDead() throws Exception {
        for (String state : List.of("QUEUED", "RUNNING", "SUCCEEDED", "CANCELLED")) {
            UUID id = insertJob("adm-retry-c", "t", state);
            mvc.perform(post("/jobs/{id}/retry", id))
                    .andExpect(status().isConflict())
                    .andExpect(jsonPath("$.detail").value(containsString("only DEAD jobs can be retried")));
        }
    }

    // ---- queues --------------------------------------------------------------------------------------------

    @Test
    void shouldCountJobsByStatusAndMeasureTheOldestWaitingJobPerQueue() throws Exception {
        // q1: one job runnable for 2 minutes, one scheduled for later (does not count as waiting), and the rest
        JDBC.sql("INSERT INTO jobs (queue, type, run_at) VALUES ('adm-q1', 't', now() - interval '120 seconds')").update();
        JDBC.sql("INSERT INTO jobs (queue, type, run_at) VALUES ('adm-q1', 't', now() + interval '1 hour')").update();
        insertJob("adm-q1", "t", "RUNNING");
        for (int i = 0; i < 3; i++) {
            insertJob("adm-q1", "t", "SUCCEEDED");
        }
        insertJob("adm-q1", "t", "DEAD");
        insertJob("adm-q1", "t", "CANCELLED");
        // q2: nothing is waiting
        insertJob("adm-q2", "t", "SUCCEEDED");
        insertJob("adm-q2", "t", "SUCCEEDED");

        String response = body(mvc.perform(get("/queues")).andExpect(status().isOk()));

        assertThat(JsonPath.<List<Integer>>read(response, "$[?(@.queue=='adm-q1')].queued")).containsExactly(2);
        assertThat(JsonPath.<List<Integer>>read(response, "$[?(@.queue=='adm-q1')].running")).containsExactly(1);
        assertThat(JsonPath.<List<Integer>>read(response, "$[?(@.queue=='adm-q1')].succeeded")).containsExactly(3);
        assertThat(JsonPath.<List<Integer>>read(response, "$[?(@.queue=='adm-q1')].dead")).containsExactly(1);
        assertThat(JsonPath.<List<Integer>>read(response, "$[?(@.queue=='adm-q1')].cancelled")).containsExactly(1);
        double age = JsonPath.<List<Number>>read(response, "$[?(@.queue=='adm-q1')].oldestQueuedAgeSeconds")
                .get(0).doubleValue();
        assertThat(age).as("age of the oldest runnable job").isBetween(120.0, 180.0);

        assertThat(JsonPath.<List<Integer>>read(response, "$[?(@.queue=='adm-q2')].succeeded")).containsExactly(2);
        assertThat(JsonPath.<List<Object>>read(response, "$[?(@.queue=='adm-q2')].oldestQueuedAgeSeconds"))
                .containsExactly((Object) null);
    }

    // ---- OpenAPI -------------------------------------------------------------------------------------------

    @Test
    void shouldDescribeTheEndpointsInOpenApiAndServeTheSwaggerUi() throws Exception {
        mvc.perform(get("/v3/api-docs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paths['/jobs']").exists())
                .andExpect(jsonPath("$.paths['/jobs/batch']").exists())
                .andExpect(jsonPath("$.paths['/jobs/{id}']").exists())
                .andExpect(jsonPath("$.paths['/jobs/{id}/cancel']").exists())
                .andExpect(jsonPath("$.paths['/jobs/{id}/retry']").exists())
                .andExpect(jsonPath("$.paths['/queues']").exists());
        mvc.perform(get("/swagger-ui/index.html")).andExpect(status().isOk());
    }
}
