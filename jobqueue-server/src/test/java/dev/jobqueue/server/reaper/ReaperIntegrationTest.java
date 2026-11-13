package dev.jobqueue.server.reaper;

import dev.jobqueue.server.PostgresIntegrationTest;
import dev.jobqueue.server.reaper.ReaperRepository.ReapResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class ReaperIntegrationTest extends PostgresIntegrationTest {

    private final ReaperRepository repository = new ReaperRepository(JDBC);
    private ReaperService service;

    @BeforeEach
    void cleanRunningJobs() {
        // The reaper looks at every RUNNING job in the database, so each test starts without any.
        JDBC.sql("DELETE FROM jobs WHERE status = 'RUNNING'").update();
    }

    @AfterEach
    void stopService() {
        if (service != null) {
            service.stop();
        }
    }

    /** A RUNNING job claimed by {@code lockedBy} on its {@code attempts}-th attempt; the lease ends {@code leaseEnds}. */
    private UUID runningJob(String queue, String lockedBy, int attempts, int maxAttempts, String leaseEnds) {
        return JDBC.sql("""
                        INSERT INTO jobs (queue, type, status, attempts, max_attempts, locked_by, locked_until, started_at)
                        VALUES (:q, 't', 'RUNNING', :a, :m, :w, now() + CAST(:l AS interval), now() - interval '5 minutes')
                        RETURNING id
                        """)
                .param("q", queue).param("a", attempts).param("m", maxAttempts).param("w", lockedBy).param("l", leaseEnds)
                .query(UUID.class).single();
    }

    private String row(UUID id) {
        return JDBC.sql("""
                        SELECT status || '|' || coalesce(locked_by, '-') || '|' || (locked_until IS NULL) || '|'
                               || coalesce(last_error, '-') || '|' || (finished_at IS NULL) || '|' || (run_at <= now())
                        FROM jobs WHERE id = :id
                        """)
                .param("id", id).query(String.class).single();
    }

    private int attemptRows(UUID id) {
        return JDBC.sql("SELECT count(*) FROM job_attempts WHERE job_id = :id")
                .param("id", id).query(Integer.class).single();
    }

    @Test
    void shouldRequeueTheJobAndRecordLeaseExpiredWhenAttemptsRemain() {
        UUID id = runningJob("reap-requeue", "dead-worker", 1, 5, "-10 seconds");

        ReapResult result = repository.reap(100);

        assertThat(result).isEqualTo(new ReapResult(1, 0));
        // QUEUED, unlocked, error noted, NOT finished, runnable right now.
        assertThat(row(id)).isEqualTo("QUEUED|-|true|lease expired|true|true");
        var attempt = JDBC.sql("""
                        SELECT a.attempt, a.worker_id, a.outcome, a.error, a.started_at = j.started_at AS same_start
                        FROM job_attempts a JOIN jobs j ON j.id = a.job_id WHERE a.job_id = :id
                        """)
                .param("id", id)
                .query((rs, n) -> List.of(rs.getInt("attempt"), rs.getString("worker_id"), rs.getString("outcome"),
                        rs.getString("error"), rs.getBoolean("same_start")))
                .single();
        // The history names the worker that lost the job, captured before the reset.
        assertThat(attempt).containsExactly(1, "dead-worker", "LEASE_EXPIRED", "lease expired", true);
    }

    @Test
    void shouldMarkDeadAndFinishWhenLeaseExpiredOnTheLastAttempt() {
        UUID id = runningJob("reap-dead", "dead-worker", 5, 5, "-10 seconds");

        ReapResult result = repository.reap(100);

        assertThat(result).isEqualTo(new ReapResult(0, 1));
        assertThat(row(id)).isEqualTo("DEAD|-|true|lease expired|false|true");
        assertThat(attemptRows(id)).isEqualTo(1);
    }

    @Test
    void shouldLeaveJobsAloneWhenTheirLeaseIsStillValid() {
        UUID id = runningJob("reap-valid", "alive-worker", 1, 5, "+60 seconds");

        ReapResult result = repository.reap(100);

        assertThat(result).isEqualTo(ReapResult.NONE);
        assertThat(row(id)).startsWith("RUNNING|alive-worker|false|");
        assertThat(attemptRows(id)).isZero();
    }

    @Test
    void shouldIgnoreJobsThatAreNotRunningEvenWithAStaleLease() {
        UUID queued = JDBC.sql("""
                        INSERT INTO jobs (queue, type, status, locked_until) VALUES ('reap-ignore', 't', 'QUEUED', now() - interval '1 hour')
                        RETURNING id
                        """).query(UUID.class).single();

        ReapResult result = repository.reap(100);

        assertThat(result).isEqualTo(ReapResult.NONE);
        assertThat(attemptRows(queued)).isZero();
    }

    @Test
    void shouldReapOnlyBatchSizeJobsPerStatementAndAllOfThemPerRun() {
        for (int i = 0; i < 5; i++) {
            runningJob("reap-batch", "dead-worker", 1, 5, "-10 seconds");
        }

        ReapResult first = repository.reap(2);
        assertThat(first.total()).isEqualTo(2);

        service = new ReaperService(repository, new ReaperProperties(Duration.ofSeconds(10), 2));
        ReapResult rest = service.reapAll(); // keeps going: 2, then 1 (not full) => stops
        assertThat(rest.total()).isEqualTo(3);
    }

    @Test
    void shouldRecoverEveryJobExactlyOnceWhenTwoReapersRunAtTheSameTime() throws Exception {
        int jobs = 300;
        JDBC.sql("""
                        INSERT INTO jobs (queue, type, status, attempts, locked_by, locked_until, started_at)
                        SELECT 'reap-concurrent', 't', 'RUNNING', 1, 'dead-worker', now() - interval '10 seconds', now() - interval '1 minute'
                        FROM generate_series(1, :n)
                        """).param("n", jobs).update();

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        List<Future<ReapResult>> futures = new ArrayList<>();
        for (int r = 0; r < 2; r++) {
            ReaperService reaper = new ReaperService(repository, new ReaperProperties(Duration.ofSeconds(10), 10));
            futures.add(pool.submit(() -> {
                start.await();
                return reaper.reapAll();
            }));
        }
        start.countDown();
        ReapResult total = ReapResult.NONE;
        for (Future<ReapResult> future : futures) {
            total = total.plus(future.get());
        }
        pool.shutdown();

        // Together they handled each job once: no job missing, none handled twice (which would also have
        // violated the UNIQUE (job_id, attempt) constraint on the history).
        assertThat(total.total()).isEqualTo(jobs);
        Integer requeued = JDBC.sql("SELECT count(*) FROM jobs WHERE queue = 'reap-concurrent' AND status = 'QUEUED'")
                .query(Integer.class).single();
        Integer historyRows = JDBC.sql("""
                        SELECT count(*) FROM job_attempts a JOIN jobs j ON j.id = a.job_id
                        WHERE j.queue = 'reap-concurrent' AND a.outcome = 'LEASE_EXPIRED'
                        """).query(Integer.class).single();
        assertThat(requeued).isEqualTo(jobs);
        assertThat(historyRows).isEqualTo(jobs);
    }

    @Test
    void shouldReapPeriodicallyWhenTheServiceIsStarted() {
        service = new ReaperService(repository, new ReaperProperties(Duration.ofMillis(100), 100));
        service.start();

        UUID id = runningJob("reap-periodic", "dead-worker", 1, 5, "-1 seconds");

        await().atMost(Duration.ofSeconds(10)).until(() -> row(id).startsWith("QUEUED|"));
        assertThat(service.isRunning()).isTrue();
    }

    @Test
    void shouldKeepRunningAfterARunFails() {
        // Force the first run to fail by pointing the reaper at a repository that throws once.
        AtomicInteger calls = new AtomicInteger();
        ReaperRepository failsOnce = new ReaperRepository(JDBC) {
            @Override
            public ReapResult reap(int batchSize) {
                if (calls.incrementAndGet() == 1) {
                    throw new IllegalStateException("simulated database hiccup");
                }
                return super.reap(batchSize);
            }
        };
        service = new ReaperService(failsOnce, new ReaperProperties(Duration.ofMillis(100), 100));
        UUID id = runningJob("reap-hiccup", "dead-worker", 1, 5, "-1 seconds");

        service.start();

        await().atMost(Duration.ofSeconds(10)).until(() -> row(id).startsWith("QUEUED|"));
        assertThat(calls.get()).isGreaterThan(1);
    }
}
