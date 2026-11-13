package dev.jobqueue.server;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchemaIntegrationTest extends PostgresIntegrationTest {

    @Test
    void shouldCreateJobsTableWithExpectedColumnsWhenMigrated() {
        List<String> columns = JDBC.sql("""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_name = 'jobs'
                        """)
                .query(String.class).list();

        assertThat(columns).containsExactlyInAnyOrder(
                "id", "queue", "type", "payload", "status", "priority", "run_at", "attempts",
                "max_attempts", "timeout_seconds", "locked_by", "locked_until", "idempotency_key",
                "last_error", "created_at", "started_at", "finished_at");
    }

    @Test
    void shouldCreateJobAttemptsTableWithUniqueAttemptPerJobWhenMigrated() {
        List<String> columns = JDBC.sql("""
                        SELECT column_name FROM information_schema.columns WHERE table_name = 'job_attempts'
                        """)
                .query(String.class).list();
        UUID jobId = JDBC.sql("INSERT INTO jobs (queue, type) VALUES ('attempts-schema', 'demo') RETURNING id")
                .query(UUID.class).single();
        JDBC.sql("INSERT INTO job_attempts (job_id, attempt, worker_id, started_at) VALUES (:id, 1, 'w', now())")
                .param("id", jobId).update();
        var duplicate = JDBC.sql(
                "INSERT INTO job_attempts (job_id, attempt, worker_id, started_at) VALUES (:id, 1, 'w', now())")
                .param("id", jobId);

        assertThat(columns).containsExactlyInAnyOrder(
                "id", "job_id", "attempt", "worker_id", "started_at", "finished_at", "outcome", "error");
        assertThatThrownBy(duplicate::update).hasMessageContaining("job_attempts_job_id_attempt_key");
    }

    @Test
    void shouldCreateTheThreePartialIndexesWhenMigrated() {
        List<String> indexes = JDBC.sql("SELECT indexname FROM pg_indexes WHERE tablename = 'jobs'")
                .query(String.class).list();

        assertThat(indexes).contains("jobs_claim_idx", "jobs_lease_idx", "jobs_idem_idx");
    }

    @Test
    void shouldApplyDefaultsWhenInsertingMinimalJob() {
        UUID id = JDBC.sql("INSERT INTO jobs (queue, type) VALUES ('default', 'demo') RETURNING id")
                .query(UUID.class).single();

        var job = JDBC.sql("SELECT status, attempts, max_attempts, payload::text AS payload FROM jobs WHERE id = :id")
                .param("id", id)
                .query((rs, n) -> List.of(rs.getString("status"), rs.getString("attempts"),
                        rs.getString("max_attempts"), rs.getString("payload")))
                .single();

        assertThat(job).containsExactly("QUEUED", "0", "5", "{}");
    }

    @Test
    void shouldRejectUnknownStatusWhenInserting() {
        var insert = JDBC.sql("INSERT INTO jobs (queue, type, status) VALUES ('default', 'demo', 'RUNING')");

        assertThatThrownBy(insert::update)
                .hasMessageContaining("jobs_status_chk");
    }

    @Test
    void shouldRejectDuplicateIdempotencyKeyWhenSameQueue() {
        JDBC.sql("INSERT INTO jobs (queue, type, idempotency_key) VALUES ('q-idem', 'demo', 'k1')").update();
        var duplicate = JDBC.sql("INSERT INTO jobs (queue, type, idempotency_key) VALUES ('q-idem', 'demo', 'k1')");

        assertThatThrownBy(duplicate::update)
                .hasMessageContaining("jobs_idem_idx");
    }
}
