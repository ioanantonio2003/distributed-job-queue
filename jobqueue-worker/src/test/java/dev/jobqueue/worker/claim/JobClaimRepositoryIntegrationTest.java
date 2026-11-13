package dev.jobqueue.worker.claim;

import dev.jobqueue.worker.PostgresIntegrationTest;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

class JobClaimRepositoryIntegrationTest extends PostgresIntegrationTest {

    private static final Duration LEASE = Duration.ofSeconds(30);

    private final JobClaimRepository repository = new JobClaimRepository(JDBC);

    @Test
    void shouldClaimHighestPriorityThenOldestRunAtFirstWhenClaimingOneAtATime() {
        insert("order", "A-prio1", 1, "now() - interval '1 minute'");
        insert("order", "B-prio5-newer", 5, "now() - interval '2 minutes'");
        insert("order", "C-prio5-older", 5, "now() - interval '3 minutes'");
        insert("order", "D-prio0", 0, "now() - interval '10 minutes'");

        List<String> claimedOrder = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            claimedOrder.addAll(repository.claim("w1", List.of("order"), LEASE, 1).stream()
                    .map(ClaimedJob::type).toList());
        }

        assertThat(claimedOrder).containsExactly("C-prio5-older", "B-prio5-newer", "A-prio1", "D-prio0");
    }

    @Test
    void shouldPickTheTopJobsWhenLimitIsSmallerThanAvailable() {
        insert("limit", "low", 0, "now()");
        insert("limit", "high-1", 9, "now() - interval '2 minutes'");
        insert("limit", "high-2", 9, "now() - interval '1 minute'");

        List<ClaimedJob> claimed = repository.claim("w1", List.of("limit"), LEASE, 2);

        // Order inside the returned list is not guaranteed, only which jobs were picked.
        assertThat(claimed).extracting(ClaimedJob::type).containsExactlyInAnyOrder("high-1", "high-2");
    }

    @Test
    void shouldNotClaimJobWhenRunAtIsInTheFuture() {
        UUID future = insert("future", "later", 0, "now() + interval '1 hour'");

        List<ClaimedJob> claimed = repository.claim("w1", List.of("future"), LEASE, 10);

        assertThat(claimed).isEmpty();
        assertThat(statusOf(future)).isEqualTo("QUEUED");
    }

    @Test
    void shouldLeaseTheJobWhenClaiming() {
        UUID id = insert("lease", "t", 0, "now()");

        List<ClaimedJob> claimed = repository.claim("worker-42", List.of("lease"), LEASE, 1);

        assertThat(claimed).singleElement().satisfies(job -> {
            assertThat(job.id()).isEqualTo(id);
            assertThat(job.attempt()).isEqualTo(1);
            assertThat(job.payload()).isEqualTo("{}");
        });
        var row = JDBC.sql("""
                        SELECT status, locked_by, attempts, started_at IS NOT NULL AS started,
                               locked_until > now() + interval '25 seconds'
                                 AND locked_until <= now() + interval '30 seconds' AS lease_ok
                        FROM jobs WHERE id = :id
                        """)
                .param("id", id)
                .query((rs, n) -> List.of(rs.getString("status"), rs.getString("locked_by"),
                        rs.getString("attempts"), String.valueOf(rs.getBoolean("started")),
                        String.valueOf(rs.getBoolean("lease_ok"))))
                .single();
        assertThat(row).containsExactly("RUNNING", "worker-42", "1", "true", "true");
    }

    @Test
    void shouldNotClaimTheSameJobTwiceWhenClaimingAgain() {
        insert("twice", "t", 0, "now()");

        List<ClaimedJob> first = repository.claim("w1", List.of("twice"), LEASE, 10);
        List<ClaimedJob> second = repository.claim("w2", List.of("twice"), LEASE, 10);

        assertThat(first).hasSize(1);
        assertThat(second).isEmpty();
    }

    @Test
    void shouldOnlyClaimFromRequestedQueuesWhenSeveralQueuesExist() {
        insert("multi-a", "a", 0, "now()");
        insert("multi-b", "b", 0, "now()");
        insert("multi-c", "c", 0, "now()");

        List<ClaimedJob> claimed = repository.claim("w1", List.of("multi-a", "multi-b"), LEASE, 10);

        assertThat(claimed).extracting(ClaimedJob::type).containsExactlyInAnyOrder("a", "b");
    }

    @Test
    void shouldClaimEachJobExactlyOnceWhenManyWorkersClaimConcurrently() throws Exception {
        int workers = 8;
        int jobs = 2000;
        JDBC.sql("INSERT INTO jobs (queue, type) SELECT 'concurrent', 't' FROM generate_series(1, :n)")
                .param("n", jobs).update();

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        List<Future<List<UUID>>> futures = new ArrayList<>();
        for (int w = 0; w < workers; w++) {
            String workerId = "worker-" + w;
            futures.add(pool.submit(() -> {
                List<UUID> mine = new ArrayList<>();
                start.await();
                List<ClaimedJob> batch;
                do {
                    batch = repository.claim(workerId, List.of("concurrent"), LEASE, 10);
                    batch.forEach(job -> mine.add(job.id()));
                } while (!batch.isEmpty());
                return mine;
            }));
        }
        start.countDown();

        List<UUID> all = new ArrayList<>();
        for (Future<List<UUID>> future : futures) {
            all.addAll(future.get());
        }
        pool.shutdown();

        Set<UUID> distinct = new HashSet<>(all);
        assertThat(all).as("total claims").hasSize(jobs);
        assertThat(distinct).as("distinct jobs claimed (no job claimed twice)").hasSize(jobs);
        Integer notExactlyOnce = JDBC.sql("""
                        SELECT count(*) FROM jobs
                        WHERE queue = 'concurrent' AND (status <> 'RUNNING' OR attempts <> 1)
                        """)
                .query(Integer.class).single();
        assertThat(notExactlyOnce).isZero();
    }

    /** Inserts one job in a fresh queue and claims it as {@code worker} with a short 5 s lease. */
    private ClaimedJob claimWithShortLease(String worker) {
        String queue = "extend-" + UUID.randomUUID();
        insert(queue, "t", 0, "now()");
        return repository.claim(worker, List.of(queue), Duration.ofSeconds(5), 1).getFirst();
    }

    /** True if the job's lease ends more than {@code seconds} from now. */
    private boolean leaseEndsAfter(UUID id, int seconds) {
        return JDBC.sql("SELECT locked_until > now() + :s * interval '1 second' FROM jobs WHERE id = :id")
                .param("s", seconds).param("id", id).query(Boolean.class).single();
    }

    @Test
    void shouldExtendTheLeaseWhenTheWorkerStillHoldsTheJob() {
        ClaimedJob job = claimWithShortLease("w1");
        assertThat(leaseEndsAfter(job.id(), 10)).isFalse(); // 5 s lease

        Set<UUID> kept = repository.extendLeases("w1", Map.of(job.id(), job.attempt()), Duration.ofSeconds(60));

        assertThat(kept).containsExactly(job.id());
        assertThat(leaseEndsAfter(job.id(), 55)).isTrue();
    }

    @Test
    void shouldReportAsLostEveryJobThatIsNoLongerHeldWhenExtendingSeveralAtOnce() {
        ClaimedJob mine = claimWithShortLease("w1");
        ClaimedJob stolen = claimWithShortLease("w1");
        ClaimedJob reclaimed = claimWithShortLease("w1");
        ClaimedJob requeued = claimWithShortLease("w1");
        JDBC.sql("UPDATE jobs SET locked_by = 'thief' WHERE id = :id").param("id", stolen.id()).update();
        JDBC.sql("UPDATE jobs SET attempts = 2 WHERE id = :id").param("id", reclaimed.id()).update();
        JDBC.sql("UPDATE jobs SET status = 'QUEUED', locked_by = NULL WHERE id = :id").param("id", requeued.id()).update();

        Set<UUID> kept = repository.extendLeases("w1", Map.of(
                mine.id(), 1, stolen.id(), 1, reclaimed.id(), 1, requeued.id(), 1), Duration.ofSeconds(60));

        assertThat(kept).containsExactly(mine.id());
        assertThat(leaseEndsAfter(mine.id(), 55)).isTrue();
        // The lost ones were left exactly as they were.
        assertThat(leaseEndsAfter(stolen.id(), 10)).isFalse();
        assertThat(leaseEndsAfter(reclaimed.id(), 10)).isFalse();
        assertThat(leaseEndsAfter(requeued.id(), 10)).isFalse();
    }

    @Test
    void shouldNotExtendWhenAnotherWorkerAsks() {
        ClaimedJob job = claimWithShortLease("w1");

        Set<UUID> kept = repository.extendLeases("w2", Map.of(job.id(), job.attempt()), Duration.ofSeconds(60));

        assertThat(kept).isEmpty();
        assertThat(leaseEndsAfter(job.id(), 10)).isFalse();
    }

    @Test
    void shouldReturnNothingWhenThereAreNoJobsToExtend() {
        assertThat(repository.extendLeases("w1", Map.of(), Duration.ofSeconds(60))).isEmpty();
    }

    private UUID insert(String queue, String type, int priority, String runAtSql) {
        // runAtSql is a literal written in this test, never user input.
        return JDBC.sql("INSERT INTO jobs (queue, type, priority, run_at) VALUES (:q, :t, :p, " + runAtSql + ") RETURNING id")
                .param("q", queue).param("t", type).param("p", priority)
                .query(UUID.class).single();
    }

    private String statusOf(UUID id) {
        return JDBC.sql("SELECT status FROM jobs WHERE id = :id").param("id", id).query(String.class).single();
    }
}
