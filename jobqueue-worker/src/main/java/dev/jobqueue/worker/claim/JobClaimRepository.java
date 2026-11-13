package dev.jobqueue.worker.claim;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Leases QUEUED jobs to a worker.
 *
 * <p>Invariant: a job is claimed by at most one worker at a time. It holds because the pick and the update
 * happen in one statement, and {@code FOR UPDATE SKIP LOCKED} makes concurrent claimers skip rows that another
 * claimer has already locked instead of waiting for them or taking them too.
 *
 * <p>No {@code @Transactional}: the statement is its own short transaction, so no lock or connection is
 * held while a handler runs.
 */
@Repository
public class JobClaimRepository {

    private final JdbcClient jdbc;

    public JobClaimRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Claims up to {@code limit} runnable jobs from {@code queues}, highest priority first, then oldest run_at.
     *
     * <p>The returned list is NOT guaranteed to be in that order (UPDATE ... RETURNING has no ordering
     * guarantee); the ORDER BY only decides which jobs are picked.
     */
    public List<ClaimedJob> claim(String workerId, List<String> queues, Duration lease, int limit) {
        return jdbc.sql("""
                        UPDATE jobs
                        SET status = 'RUNNING',
                            locked_by = :workerId,
                            -- The database clock decides expiry, never the Java clock.
                            locked_until = now() + :leaseSeconds * interval '1 second',
                            -- Incremented at claim time, so a crash counts as an attempt, and the new value
                            -- is the fencing token for this particular lease.
                            attempts = attempts + 1,
                            started_at = now()
                        WHERE id IN (
                            SELECT id FROM jobs
                            WHERE status = 'QUEUED'
                              AND queue = ANY(:queues)
                              AND run_at <= now()
                            ORDER BY priority DESC, run_at
                            LIMIT :limit
                            -- Rows locked by another claimer are skipped, not waited for.
                            FOR UPDATE SKIP LOCKED
                        )
                        RETURNING id, queue, type, payload::text AS payload, attempts, max_attempts,
                                  timeout_seconds, idempotency_key
                        """)
                .param("workerId", workerId)
                .param("leaseSeconds", lease.toSeconds())
                // An array, not a List: a List would be expanded into (?, ?, ...) and break ANY(...).
                .param("queues", queues.toArray(new String[0]))
                .param("limit", limit)
                .query((rs, rowNum) -> new ClaimedJob(
                        rs.getObject("id", UUID.class),
                        rs.getString("queue"),
                        rs.getString("type"),
                        rs.getString("payload"),
                        rs.getInt("attempts"),
                        rs.getInt("max_attempts"),
                        rs.getInt("timeout_seconds"),
                        rs.getString("idempotency_key")))
                .list();
    }
}
