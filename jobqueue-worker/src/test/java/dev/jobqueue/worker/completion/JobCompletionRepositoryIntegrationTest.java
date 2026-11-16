package dev.jobqueue.worker.completion;

import dev.jobqueue.worker.PostgresIntegrationTest;
import dev.jobqueue.worker.claim.ClaimedJob;
import dev.jobqueue.worker.claim.JobClaimRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class JobCompletionRepositoryIntegrationTest extends PostgresIntegrationTest {

    private final JobClaimRepository claimRepository = new JobClaimRepository(JDBC);
    private final JobCompletionRepository completionRepository = new JobCompletionRepository(JDBC);

    /** Inserts one job in a fresh queue and claims it as {@code workerId}. */
    private ClaimedJob claimedBy(String workerId) {
        String queue = "complete-" + UUID.randomUUID();
        JDBC.sql("INSERT INTO jobs (queue, type) VALUES (:q, 't')").param("q", queue).update();
        return claimRepository.claim(workerId, queue, Duration.ofSeconds(30), 1).getFirst();
    }

    private String snapshot(UUID id) {
        return JDBC.sql("""
                        SELECT status || '|' || coalesce(locked_by, '-') || '|' || attempts || '|'
                               || (finished_at IS NULL)
                        FROM jobs WHERE id = :id
                        """)
                .param("id", id).query(String.class).single();
    }

    private int attemptRows(UUID id) {
        return JDBC.sql("SELECT count(*) FROM job_attempts WHERE job_id = :id")
                .param("id", id).query(Integer.class).single();
    }

    @Test
    void shouldMarkSucceededAndRecordTheAttemptWhenWorkerStillHoldsTheJob() {
        ClaimedJob job = claimedBy("w1");

        boolean completed = completionRepository.complete(job.id(), "w1", job.attempt());

        assertThat(completed).isTrue();
        var jobRow = JDBC.sql("""
                        SELECT status, finished_at IS NOT NULL AS finished,
                               locked_by IS NULL AS unlocked, locked_until IS NULL AS no_lease
                        FROM jobs WHERE id = :id
                        """)
                .param("id", job.id())
                .query((rs, n) -> List.of(rs.getString("status"), rs.getBoolean("finished"),
                        rs.getBoolean("unlocked"), rs.getBoolean("no_lease")))
                .single();
        assertThat(jobRow).containsExactly("SUCCEEDED", true, true, true);

        var attempt = JDBC.sql("""
                        SELECT a.attempt, a.worker_id, a.outcome, a.error, a.finished_at IS NOT NULL AS finished,
                               a.started_at = j.started_at AS same_start
                        FROM job_attempts a JOIN jobs j ON j.id = a.job_id WHERE a.job_id = :id
                        """)
                .param("id", job.id())
                .query((rs, n) -> List.of(rs.getInt("attempt"), rs.getString("worker_id"),
                        rs.getString("outcome"), String.valueOf(rs.getString("error")),
                        rs.getBoolean("finished"), rs.getBoolean("same_start")))
                .single();
        assertThat(attempt).containsExactly(1, "w1", "SUCCEEDED", "null", true, true);
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {
            // stolen by another worker
            "UPDATE jobs SET locked_by = 'thief' WHERE id = :id",
            // claimed again after a lease expiry: same worker, newer attempt
            "UPDATE jobs SET attempts = 2 WHERE id = :id",
            // put back in the queue (as a reaper would)
            "UPDATE jobs SET status = 'QUEUED', locked_by = NULL, locked_until = NULL WHERE id = :id",
            // declared dead meanwhile
            "UPDATE jobs SET status = 'DEAD', locked_by = NULL, locked_until = NULL WHERE id = :id"})
    void shouldRejectCompletionAndChangeNothingWhenTheJobWasTakenAway(String takeAwaySql) {
        ClaimedJob job = claimedBy("w1");
        JDBC.sql(takeAwaySql).param("id", job.id()).update();
        String before = snapshot(job.id());

        boolean completed = completionRepository.complete(job.id(), "w1", job.attempt());

        assertThat(completed).isFalse();
        assertThat(snapshot(job.id())).isEqualTo(before);
        assertThat(attemptRows(job.id())).isZero();
    }

    @Test
    void shouldRejectCompletionWhenAnotherWorkerTriesToFinishTheJob() {
        ClaimedJob job = claimedBy("w1");

        boolean completed = completionRepository.complete(job.id(), "w2", job.attempt());

        assertThat(completed).isFalse();
        assertThat(snapshot(job.id())).startsWith("RUNNING|w1|1|");
        assertThat(attemptRows(job.id())).isZero();
    }

    @Test
    void shouldRejectCompletionWhenAttemptNumberIsStale() {
        ClaimedJob job = claimedBy("w1");

        boolean completed = completionRepository.complete(job.id(), "w1", job.attempt() - 1);

        assertThat(completed).isFalse();
        assertThat(snapshot(job.id())).startsWith("RUNNING|w1|1|");
    }

    @Test
    void shouldRecordOnlyOneAttemptWhenCompletedTwice() {
        ClaimedJob job = claimedBy("w1");

        boolean first = completionRepository.complete(job.id(), "w1", job.attempt());
        boolean second = completionRepository.complete(job.id(), "w1", job.attempt());

        assertThat(first).isTrue();
        assertThat(second).isFalse();
        assertThat(attemptRows(job.id())).isEqualTo(1);
    }

    @Test
    void shouldReturnFalseWhenJobDoesNotExist() {
        assertThat(completionRepository.complete(UUID.randomUUID(), "w1", 1)).isFalse();
    }

    // ---- release: giving jobs back on a graceful shutdown ------------------------------------------------------

    @Test
    void shouldGiveTheJobBackWithoutUsingAnAttemptWhenTheWorkerStillHoldsIt() {
        ClaimedJob job = claimedBy("w1"); // attempts 0 -> 1
        assertThat(snapshot(job.id())).startsWith("RUNNING|w1|1|");

        int released = completionRepository.release("w1", Map.of(job.id(), job.attempt()));

        assertThat(released).isEqualTo(1);
        assertThat(snapshot(job.id())).isEqualTo("QUEUED|-|0|true"); // attempts back to 0, not finished
        assertThat(attemptRows(job.id())).as("no history row for the unfinished attempt").isZero();
        // Claimed again, it is attempt 1 once more: the released attempt was never counted.
        String queue = JDBC.sql("SELECT queue FROM jobs WHERE id = :id").param("id", job.id())
                .query(String.class).single();
        assertThat(claimRepository.claim("w2", queue, Duration.ofSeconds(30), 1))
                .singleElement().satisfies(again -> assertThat(again.attempt()).isEqualTo(1));
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {
            "UPDATE jobs SET locked_by = 'thief' WHERE id = :id",
            "UPDATE jobs SET attempts = 2 WHERE id = :id",
            "UPDATE jobs SET status = 'QUEUED', locked_by = NULL, locked_until = NULL WHERE id = :id",
            "UPDATE jobs SET status = 'DEAD', locked_by = NULL, locked_until = NULL WHERE id = :id"})
    void shouldLeaveTheJobAloneWhenItIsNotHeldByThisWorkerAnyMore(String takeAwaySql) {
        ClaimedJob job = claimedBy("w1");
        JDBC.sql(takeAwaySql).param("id", job.id()).update();
        String before = snapshot(job.id());

        int released = completionRepository.release("w1", Map.of(job.id(), job.attempt()));

        assertThat(released).isZero();
        assertThat(snapshot(job.id())).isEqualTo(before);
    }

    @Test
    void shouldNotReleaseAJobHeldByAnotherWorker() {
        ClaimedJob job = claimedBy("w1");

        int released = completionRepository.release("w2", Map.of(job.id(), job.attempt()));

        assertThat(released).isZero();
        assertThat(snapshot(job.id())).startsWith("RUNNING|w1|1|");
    }

    @Test
    void shouldReleaseOnlyTheGivenJobsWhenSeveralAreRunning() {
        ClaimedJob first = claimedBy("w1");
        ClaimedJob second = claimedBy("w1");
        ClaimedJob untouched = claimedBy("w1");

        int released = completionRepository.release("w1",
                Map.of(first.id(), first.attempt(), second.id(), second.attempt()));

        assertThat(released).isEqualTo(2);
        assertThat(snapshot(first.id())).startsWith("QUEUED|");
        assertThat(snapshot(second.id())).startsWith("QUEUED|");
        assertThat(snapshot(untouched.id())).startsWith("RUNNING|w1|1|");
    }

    @Test
    void shouldReleaseNothingWhenThereAreNoJobs() {
        assertThat(completionRepository.release("w1", Map.of())).isZero();
    }
}
