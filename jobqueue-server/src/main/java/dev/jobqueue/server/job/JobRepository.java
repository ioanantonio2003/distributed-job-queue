package dev.jobqueue.server.job;

import dev.jobqueue.common.JobStatus;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Explicit-SQL access to the {@code jobs} table. No JPA: on the hot path we want to see and control
 * every statement.
 */
@Repository
public class JobRepository {

    // payload is cast to text on read so Java sees plain JSON, without a driver-specific PGobject.
    private static final String COLUMNS = """
            id, queue, type, payload::text AS payload, status, priority, run_at, attempts, max_attempts,
            timeout_seconds, locked_by, locked_until, idempotency_key, last_error,
            created_at, started_at, finished_at
            """;

    private static final RowMapper<Job> JOB_MAPPER = (rs, rowNum) -> new Job(
            rs.getObject("id", UUID.class),
            rs.getString("queue"),
            rs.getString("type"),
            rs.getString("payload"),
            JobStatus.valueOf(rs.getString("status")),
            rs.getInt("priority"),
            instant(rs, "run_at"),
            rs.getInt("attempts"),
            rs.getInt("max_attempts"),
            rs.getInt("timeout_seconds"),
            rs.getString("locked_by"),
            instant(rs, "locked_until"),
            rs.getString("idempotency_key"),
            rs.getString("last_error"),
            instant(rs, "created_at"),
            instant(rs, "started_at"),
            instant(rs, "finished_at"));

    private final JdbcClient jdbc;
    // JdbcClient has no batch API, so the batch insert goes through the JdbcTemplate it is built on.
    private final JdbcTemplate jdbcTemplate;

    public JobRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.jdbc = JdbcClient.create(jdbcTemplate);
    }

    /**
     * Inserts a job and returns its generated id, or empty if a job with the same (queue, idempotency_key)
     * already exists. Status and created_at take the column defaults. run_at is the given absolute time, or
     * {@code now() + delay} when only a delay was given: in both cases the database clock is the reference.
     *
     * <p>The uniqueness rule is enforced by the partial unique index {@code jobs_idem_idx}, not by Java:
     * "check, then insert" would let two concurrent requests both pass the check.
     */
    public Optional<UUID> insert(NewJob job) {
        return jdbc.sql("""
                        INSERT INTO jobs (queue, type, payload, priority, max_attempts, timeout_seconds, idempotency_key, run_at)
                        VALUES (:queue, :type, CAST(:payload AS jsonb), :priority, :maxAttempts, :timeoutSeconds, :idempotencyKey,
                                -- now() is the database clock: the delay is measured on the same clock that
                                -- decides, at claim time, whether run_at has been reached.
                                COALESCE(CAST(:runAt AS timestamptz), now() + :delaySeconds * interval '1 second'))
                        -- The conflict target repeats the index predicate so Postgres can match the partial index.
                        -- On conflict nothing is inserted and RETURNING yields no row.
                        ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL
                        DO NOTHING
                        RETURNING id
                        """)
                .param("queue", job.queue())
                .param("type", job.type())
                // CAST: the driver sends the parameter as text; Postgres will not convert text to jsonb implicitly.
                .param("payload", job.payload())
                .param("priority", job.priority())
                .param("maxAttempts", job.maxAttempts())
                .param("timeoutSeconds", job.timeoutSeconds())
                .param("idempotencyKey", job.idempotencyKey())
                // pgjdbc does not accept java.time.Instant as a parameter; OffsetDateTime is the supported type.
                .param("runAt", job.runAt() == null ? null : job.runAt().atOffset(ZoneOffset.UTC))
                .param("delaySeconds", job.delaySeconds())
                .query(UUID.class)
                .optional();
    }

    /**
     * The current time of the database server: the clock that decides when a job becomes runnable.
     */
    public Instant databaseNow() {
        return jdbc.sql("SELECT now()").query(OffsetDateTime.class).single().toInstant();
    }

    /**
     * Inserts all jobs in one JDBC batch (one round trip instead of one per job) and returns their ids in order.
     * Ids are generated here, because a batch cannot use RETURNING. Atomicity comes from the caller's transaction.
     * Idempotency keys are not handled: a duplicate key would fail the whole batch.
     */
    public List<UUID> insertBatch(List<NewJob> jobs) {
        List<UUID> ids = jobs.stream().map(job -> UUID.randomUUID()).toList();
        jdbcTemplate.batchUpdate("""
                INSERT INTO jobs (id, queue, type, payload, priority, max_attempts, timeout_seconds, run_at)
                VALUES (?, ?, ?, CAST(? AS jsonb), ?, ?, ?,
                        COALESCE(CAST(? AS timestamptz), now() + ? * interval '1 second'))
                """, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                NewJob job = jobs.get(i);
                ps.setObject(1, ids.get(i));
                ps.setString(2, job.queue());
                ps.setString(3, job.type());
                ps.setString(4, job.payload());
                ps.setInt(5, job.priority());
                ps.setInt(6, job.maxAttempts());
                ps.setInt(7, job.timeoutSeconds());
                if (job.runAt() == null) {
                    ps.setNull(8, Types.TIMESTAMP_WITH_TIMEZONE);
                } else {
                    ps.setObject(8, job.runAt().atOffset(ZoneOffset.UTC));
                }
                ps.setInt(9, job.delaySeconds());
            }

            @Override
            public int getBatchSize() {
                return jobs.size();
            }
        });
        return ids;
    }

    public Optional<Job> findById(UUID id) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM jobs WHERE id = :id")
                .param("id", id)
                .query(JOB_MAPPER)
                .optional();
    }

    // The optional filters: a null parameter means "do not filter on this". CAST gives the null a type, which
    // PostgreSQL needs to plan the statement. Meant for the admin screens, not for the hot path: these filters
    // are not served by the claim indexes.
    private static final String FILTER = """
            (CAST(:queue AS text) IS NULL OR queue = :queue)
            AND (CAST(:status AS text) IS NULL OR status = :status)
            AND (CAST(:type AS text) IS NULL OR type = :type)
            """;

    /**
     * One page of jobs, newest first. OFFSET paging is simple but reads and discards {@code offset} rows, so
     * very deep pages get slower; fine for an admin view, not for walking millions of jobs.
     */
    public List<Job> findPage(String queue, JobStatus status, String type, int limit, int offset) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM jobs WHERE " + FILTER
                        + " ORDER BY created_at DESC, id LIMIT :limit OFFSET :offset")
                .param("queue", queue)
                .param("status", status == null ? null : status.name())
                .param("type", type)
                .param("limit", limit)
                .param("offset", offset)
                .query(JOB_MAPPER)
                .list();
    }

    public long count(String queue, JobStatus status, String type) {
        return jdbc.sql("SELECT count(*) FROM jobs WHERE " + FILTER)
                .param("queue", queue)
                .param("status", status == null ? null : status.name())
                .param("type", type)
                .query(Long.class)
                .single();
    }

    public List<AttemptResponse> findAttempts(UUID jobId) {
        return jdbc.sql("""
                        SELECT attempt, worker_id, started_at, finished_at, outcome, error
                        FROM job_attempts WHERE job_id = :id ORDER BY attempt
                        """)
                .param("id", jobId)
                .query((rs, rowNum) -> new AttemptResponse(
                        rs.getInt("attempt"),
                        rs.getString("worker_id"),
                        instant(rs, "started_at"),
                        instant(rs, "finished_at"),
                        rs.getString("outcome"),
                        rs.getString("error")))
                .list();
    }

    /**
     * QUEUED to CANCELLED, atomically: the status check and the change are one statement, so a worker that claims
     * the job at the same moment either wins (this returns false) or loses (the job is never claimed).
     *
     * @return true if the job was QUEUED and is now CANCELLED
     */
    public boolean cancel(UUID id) {
        return jdbc.sql("""
                        UPDATE jobs SET status = 'CANCELLED', finished_at = now()
                        WHERE id = :id AND status = 'QUEUED'
                        """)
                .param("id", id).update() == 1;
    }

    /**
     * DEAD to QUEUED, atomically, runnable now. {@code attempts} is kept, because job_attempts already holds one
     * row per attempt (UNIQUE (job_id, attempt)) and numbering must go on from there; instead max_attempts is
     * raised by the current value, so the job gets a fresh full set of attempts.
     *
     * @return true if the job was DEAD and is queued again
     */
    public boolean retry(UUID id) {
        return jdbc.sql("""
                        UPDATE jobs
                        SET status = 'QUEUED', max_attempts = attempts + max_attempts, run_at = now(),
                            finished_at = NULL, locked_by = NULL, locked_until = NULL
                        WHERE id = :id AND status = 'DEAD'
                        """)
                .param("id", id).update() == 1;
    }

    /**
     * Jobs per queue and status, and how long the oldest runnable QUEUED job has waited, in one pass over the table
     * (a single GROUP BY, not one query per queue). It reads every row, so it is for occasional admin use.
     */
    public List<QueueStats> queueStats() {
        return jdbc.sql("""
                        SELECT queue,
                               count(*) FILTER (WHERE status = 'QUEUED')    AS queued,
                               count(*) FILTER (WHERE status = 'RUNNING')   AS running,
                               count(*) FILTER (WHERE status = 'SUCCEEDED') AS succeeded,
                               count(*) FILTER (WHERE status = 'DEAD')      AS dead,
                               count(*) FILTER (WHERE status = 'CANCELLED') AS cancelled,
                               -- Only jobs already runnable have been "waiting"; the database clock measures it.
                               -- EXTRACT returns numeric, which the driver will not hand over as a Double.
                               CAST(EXTRACT(EPOCH FROM now() - min(run_at) FILTER (WHERE status = 'QUEUED' AND run_at <= now()))
                                    AS double precision) AS oldest_queued_age_seconds
                        FROM jobs
                        GROUP BY queue
                        ORDER BY queue
                        """)
                .query((rs, rowNum) -> new QueueStats(
                        rs.getString("queue"),
                        rs.getLong("queued"),
                        rs.getLong("running"),
                        rs.getLong("succeeded"),
                        rs.getLong("dead"),
                        rs.getLong("cancelled"),
                        rs.getObject("oldest_queued_age_seconds", Double.class)))
                .list();
    }

    // Uses jobs_idem_idx; used after an insert hit a conflict, to return the job that won.
    public Optional<Job> findByIdempotencyKey(String queue, String idempotencyKey) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM jobs WHERE queue = :queue AND idempotency_key = :key")
                .param("queue", queue)
                .param("key", idempotencyKey)
                .query(JOB_MAPPER)
                .optional();
    }

    // getObject(..., OffsetDateTime) is the JDBC-standard way to read timestamptz; NULL becomes null.
    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }
}
