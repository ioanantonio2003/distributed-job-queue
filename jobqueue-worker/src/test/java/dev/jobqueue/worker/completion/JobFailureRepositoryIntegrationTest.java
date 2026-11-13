package dev.jobqueue.worker.completion;

import dev.jobqueue.common.AttemptOutcome;
import dev.jobqueue.common.JobStatus;
import dev.jobqueue.worker.PostgresIntegrationTest;
import dev.jobqueue.worker.claim.ClaimedJob;
import dev.jobqueue.worker.claim.JobClaimRepository;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class JobFailureRepositoryIntegrationTest extends PostgresIntegrationTest {

    private static final Duration LEASE = Duration.ofSeconds(30);

    private final JobClaimRepository claimRepository = new JobClaimRepository(JDBC);
    private final JobCompletionRepository completionRepository = new JobCompletionRepository(JDBC);

    private record Claim(String queue, ClaimedJob job) {
    }

    /** Inserts a job with the given max_attempts in a fresh queue and claims it as w1. */
    private Claim claim(int maxAttempts) {
        String queue = "fail-" + UUID.randomUUID();
        JDBC.sql("INSERT INTO jobs (queue, type, max_attempts) VALUES (:q, 't', :m)")
                .param("q", queue).param("m", maxAttempts).update();
        return new Claim(queue, claimRepository.claim("w1", List.of(queue), LEASE, 1).getFirst());
    }

    private String row(UUID id) {
        return JDBC.sql("""
                        SELECT status || '|' || coalesce(locked_by, '-') || '|' || attempts || '|'
                               || coalesce(last_error, '-') || '|' || (finished_at IS NULL)
                        FROM jobs WHERE id = :id
                        """)
                .param("id", id).query(String.class).single();
    }

    @Test
    void shouldRequeueWithDelayAndRecordFailedAttemptWhenAttemptsRemain() {
        Claim claim = claim(3);

        Optional<JobStatus> status = completionRepository.fail(claim.job().id(), "w1", 1, "boom",
                false, Duration.ofSeconds(60), AttemptOutcome.FAILED);

        assertThat(status).contains(JobStatus.QUEUED);
        // QUEUED, unlocked, error kept, and NOT finished (finished_at stays null while a retry is pending).
        assertThat(row(claim.job().id())).isEqualTo("QUEUED|-|1|boom|true");
        Boolean delayed = JDBC.sql("""
                        SELECT run_at > now() + interval '55 seconds' AND run_at <= now() + interval '60 seconds'
                        FROM jobs WHERE id = :id
                        """).param("id", claim.job().id()).query(Boolean.class).single();
        assertThat(delayed).isTrue();
        var attempt = JDBC.sql("SELECT attempt, worker_id, outcome, error FROM job_attempts WHERE job_id = :id")
                .param("id", claim.job().id())
                .query((rs, n) -> List.of(rs.getInt("attempt"), rs.getString("worker_id"),
                        rs.getString("outcome"), rs.getString("error")))
                .single();
        assertThat(attempt).containsExactly(1, "w1", "FAILED", "boom");
    }

    @Test
    void shouldNotClaimTheJobAgainUntilTheDelayHasPassed() {
        Claim delayed = claim(3);
        completionRepository.fail(delayed.job().id(), "w1", 1, "boom", false, Duration.ofSeconds(60), AttemptOutcome.FAILED);

        assertThat(claimRepository.claim("w1", List.of(delayed.queue()), LEASE, 1)).isEmpty();

        Claim immediate = claim(3);
        completionRepository.fail(immediate.job().id(), "w1", 1, "boom", false, Duration.ZERO, AttemptOutcome.FAILED);

        List<ClaimedJob> again = claimRepository.claim("w1", List.of(immediate.queue()), LEASE, 1);
        assertThat(again).singleElement().satisfies(job -> assertThat(job.attempt()).isEqualTo(2));
    }

    @Test
    void shouldMarkDeadAndFinishWhenAttemptsAreUsedUp() {
        Claim claim = claim(1); // the first attempt is also the last

        Optional<JobStatus> status = completionRepository.fail(claim.job().id(), "w1", 1, "last",
                false, Duration.ofSeconds(60), AttemptOutcome.FAILED);

        assertThat(status).contains(JobStatus.DEAD);
        assertThat(row(claim.job().id())).isEqualTo("DEAD|-|1|last|false"); // finished_at is set
    }

    @Test
    void shouldMarkDeadImmediatelyWhenFailureIsPermanentEvenIfAttemptsRemain() {
        Claim claim = claim(5);

        Optional<JobStatus> status = completionRepository.fail(claim.job().id(), "w1", 1, "bad payload",
                true, Duration.ZERO, AttemptOutcome.FAILED);

        assertThat(status).contains(JobStatus.DEAD);
        assertThat(row(claim.job().id())).isEqualTo("DEAD|-|1|bad payload|false");
    }

    @Test
    void shouldIgnoreFailureAndChangeNothingWhenTheJobWasTakenAway() {
        Claim claim = claim(5);
        JDBC.sql("UPDATE jobs SET locked_by = 'thief', attempts = 2 WHERE id = :id")
                .param("id", claim.job().id()).update();
        String before = row(claim.job().id());

        Optional<JobStatus> status = completionRepository.fail(claim.job().id(), "w1", 1, "late failure",
                false, Duration.ZERO, AttemptOutcome.FAILED);

        assertThat(status).isEmpty();
        assertThat(row(claim.job().id())).isEqualTo(before);
        Integer attemptRows = JDBC.sql("SELECT count(*) FROM job_attempts WHERE job_id = :id")
                .param("id", claim.job().id()).query(Integer.class).single();
        assertThat(attemptRows).isZero();
    }

    @Test
    void shouldIgnoreFailureWhenAnotherWorkerReportsIt() {
        Claim claim = claim(5);

        Optional<JobStatus> status = completionRepository.fail(claim.job().id(), "w2", 1, "not mine",
                false, Duration.ZERO, AttemptOutcome.FAILED);

        assertThat(status).isEmpty();
        assertThat(row(claim.job().id())).startsWith("RUNNING|w1|1|");
    }

    @Test
    void shouldRecordTimedOutOutcomeAndRequeueWhenTheAttemptTimedOut() {
        Claim claim = claim(3);

        Optional<JobStatus> status = completionRepository.fail(claim.job().id(), "w1", 1, "handler exceeded timeout",
                false, Duration.ZERO, AttemptOutcome.TIMED_OUT);

        assertThat(status).contains(JobStatus.QUEUED);
        String outcome = JDBC.sql("SELECT outcome FROM job_attempts WHERE job_id = :id")
                .param("id", claim.job().id()).query(String.class).single();
        assertThat(outcome).isEqualTo("TIMED_OUT");
    }
}
