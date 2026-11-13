package dev.jobqueue.server.job;

import dev.jobqueue.common.JobStatus;
import dev.jobqueue.server.PostgresIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobRepositoryIntegrationTest extends PostgresIntegrationTest {

    private final JobRepository repository = new JobRepository(JDBC);

    @Test
    void shouldReturnInsertedJobWhenFindingById() {
        NewJob newJob = new NewJob("emails", "send-welcome", "{\"to\":\"a@b.c\"}", 3, 7, 30, "welcome-1");

        UUID id = repository.insert(newJob);
        Job job = repository.findById(id).orElseThrow();

        assertThat(job.id()).isEqualTo(id);
        assertThat(job.queue()).isEqualTo("emails");
        assertThat(job.type()).isEqualTo("send-welcome");
        assertThat(job.payload()).contains("\"to\"").contains("a@b.c");
        assertThat(job.priority()).isEqualTo(3);
        assertThat(job.maxAttempts()).isEqualTo(7);
        assertThat(job.timeoutSeconds()).isEqualTo(30);
        assertThat(job.idempotencyKey()).isEqualTo("welcome-1");
    }

    @Test
    void shouldSetDatabaseDefaultsWhenInserting() {
        UUID id = repository.insert(new NewJob("defaults", "demo", "{}", 0, 5, 60, null));

        Job job = repository.findById(id).orElseThrow();

        assertThat(job.status()).isEqualTo(JobStatus.QUEUED);
        assertThat(job.attempts()).isZero();
        assertThat(job.createdAt()).isNotNull();
        assertThat(job.runAt()).isNotNull();
        assertThat(job.idempotencyKey()).isNull();
        assertThat(job.lockedBy()).isNull();
        assertThat(job.lockedUntil()).isNull();
        assertThat(job.startedAt()).isNull();
        assertThat(job.finishedAt()).isNull();
    }

    @Test
    void shouldReturnEmptyWhenJobDoesNotExist() {
        Optional<Job> job = repository.findById(UUID.randomUUID());

        assertThat(job).isEmpty();
    }

    @Test
    void shouldRejectInsertWhenIdempotencyKeyAlreadyUsedInQueue() {
        repository.insert(new NewJob("repo-idem", "demo", "{}", 0, 5, 60, "same-key"));

        assertThatThrownBy(() -> repository.insert(new NewJob("repo-idem", "demo", "{}", 0, 5, 60, "same-key")))
                .isInstanceOf(DuplicateKeyException.class);
    }
}
