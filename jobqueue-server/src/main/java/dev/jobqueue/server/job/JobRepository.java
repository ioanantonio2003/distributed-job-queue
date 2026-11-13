package dev.jobqueue.server.job;

import dev.jobqueue.common.JobStatus;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
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

    public JobRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Inserts a job and returns its generated id. Status, run_at and created_at take the column defaults,
     * so the database clock decides them.
     */
    public UUID insert(NewJob job) {
        return jdbc.sql("""
                        INSERT INTO jobs (queue, type, payload, priority, max_attempts, timeout_seconds, idempotency_key)
                        VALUES (:queue, :type, CAST(:payload AS jsonb), :priority, :maxAttempts, :timeoutSeconds, :idempotencyKey)
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
                .single();
    }

    public Optional<Job> findById(UUID id) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM jobs WHERE id = :id")
                .param("id", id)
                .query(JOB_MAPPER)
                .optional();
    }

    // getObject(..., OffsetDateTime) is the JDBC-standard way to read timestamptz; NULL becomes null.
    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }
}
