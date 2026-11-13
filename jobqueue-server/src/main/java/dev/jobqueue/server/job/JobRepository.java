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
import java.time.Instant;
import java.time.OffsetDateTime;
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
     * already exists. Status, run_at and created_at take the column defaults, so the database clock decides them.
     *
     * <p>The uniqueness rule is enforced by the partial unique index {@code jobs_idem_idx}, not by Java:
     * "check, then insert" would let two concurrent requests both pass the check.
     */
    public Optional<UUID> insert(NewJob job) {
        return jdbc.sql("""
                        INSERT INTO jobs (queue, type, payload, priority, max_attempts, timeout_seconds, idempotency_key)
                        VALUES (:queue, :type, CAST(:payload AS jsonb), :priority, :maxAttempts, :timeoutSeconds, :idempotencyKey)
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
                .query(UUID.class)
                .optional();
    }

    /**
     * Inserts all jobs in one JDBC batch (one round trip instead of one per job) and returns their ids in order.
     * Ids are generated here, because a batch cannot use RETURNING. Atomicity comes from the caller's transaction.
     * Idempotency keys are not handled: a duplicate key would fail the whole batch.
     */
    public List<UUID> insertBatch(List<NewJob> jobs) {
        List<UUID> ids = jobs.stream().map(job -> UUID.randomUUID()).toList();
        jdbcTemplate.batchUpdate("""
                INSERT INTO jobs (id, queue, type, payload, priority, max_attempts, timeout_seconds)
                VALUES (?, ?, ?, CAST(? AS jsonb), ?, ?, ?)
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
