package dev.jobqueue.worker.completion;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.util.UUID;

/**
 * Records the successful end of a job attempt.
 *
 * <p>Invariant (fencing): only the worker that currently holds the lease can finish the job. The lease is
 * identified by (locked_by, attempts) while the status is RUNNING; a worker that lost the job, for example
 * because its lease expired and another worker claimed it, matches nothing and changes nothing.
 */
@Repository
public class JobCompletionRepository {

    private final JdbcClient jdbc;

    public JobCompletionRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Marks the job SUCCEEDED and writes its job_attempts row, in one statement.
     *
     * @return true if this worker still held the job and it is now SUCCEEDED; false if the lease was lost
     *         (nothing was changed and the caller must ignore its result)
     */
    public boolean complete(UUID jobId, String workerId, int attempt) {
        // One statement = one implicit transaction: the UPDATE and the INSERT both happen or neither does,
        // without an explicit @Transactional. The INSERT selects from "done", so it produces a row only when
        // the fenced UPDATE matched one. The affected-row count is therefore 1 on success and 0 when fenced out.
        int rows = jdbc.sql("""
                        WITH done AS (
                            UPDATE jobs
                            SET status = 'SUCCEEDED', finished_at = now(), locked_by = NULL, locked_until = NULL
                            -- The fencing check: same worker AND same attempt number AND still RUNNING.
                            WHERE id = :jobId AND locked_by = :workerId AND attempts = :attempt AND status = 'RUNNING'
                            RETURNING id, attempts, started_at
                        )
                        INSERT INTO job_attempts (job_id, attempt, worker_id, started_at, finished_at, outcome)
                        SELECT id, attempts, :workerId, started_at, now(), 'SUCCEEDED' FROM done
                        """)
                .param("jobId", jobId)
                .param("workerId", workerId)
                .param("attempt", attempt)
                .update();
        return rows == 1;
    }
}
