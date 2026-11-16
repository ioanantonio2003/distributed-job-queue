package dev.jobqueue.server.job;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Business rules for jobs: validates the payload, delegates storage to {@link JobRepository}
 * and converts rows into API responses.
 */
@Service
public class JobService {

    // A job payload is a pointer to work (ids, small parameters), not a data transport.
    // Large payloads bloat the table, the indexes' heap fetches and every claim.
    static final int MAX_PAYLOAD_BYTES = 64 * 1024;

    private final JobRepository repository;
    private final ObjectMapper objectMapper;

    public JobService(JobRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    public EnqueueResult enqueue(EnqueueRequest request) {
        Optional<UUID> id = repository.insert(toNewJob(request, repository::databaseNow));
        if (id.isPresent()) {
            return new EnqueueResult(get(id.get()), true);
        }
        // Empty means the idempotency key already exists: return the job that got there first.
        // The insert waited for any concurrent in-flight insert of the same key to finish, and this SELECT
        // is a new statement (READ COMMITTED), so it sees that committed row.
        Job existing = repository.findByIdempotencyKey(request.queue(), request.idempotencyKey())
                .orElseThrow(() -> new IllegalStateException("Idempotent job vanished after conflict"));
        return new EnqueueResult(toResponse(existing), false);
    }

    /**
     * All-or-nothing: if any insert fails, none of the jobs is stored.
     */
    @Transactional
    public BatchEnqueueResponse enqueueBatch(BatchEnqueueRequest request) {
        // Silently ignoring a key would break the caller's expectation of "no duplicates", so refuse it.
        if (request.jobs().stream().anyMatch(job -> job.idempotencyKey() != null)) {
            throw new InvalidJobException("idempotencyKey is not supported in batch requests");
        }
        // The database time is only needed to bound runAt, and one reading serves the whole batch.
        Instant[] databaseNow = new Instant[1];
        Supplier<Instant> clock = () -> {
            if (databaseNow[0] == null) {
                databaseNow[0] = repository.databaseNow();
            }
            return databaseNow[0];
        };
        List<NewJob> newJobs = request.jobs().stream().map(job -> toNewJob(job, clock)).toList();
        return new BatchEnqueueResponse(repository.insertBatch(newJobs));
    }

    public JobResponse get(UUID id) {
        Job job = repository.findById(id).orElseThrow(() -> new JobNotFoundException(id));
        return toResponse(job);
    }

    private NewJob toNewJob(EnqueueRequest request, Supplier<Instant> databaseNow) {
        if (request.delaySeconds() != null && request.runAt() != null) {
            throw new InvalidJobException("give either delaySeconds or runAt, not both");
        }
        if (request.runAt() != null) {
            // "Now" comes from the database, the same clock that will decide when the job may start.
            Instant latest = databaseNow.get().plusSeconds(EnqueueRequest.MAX_DELAY_SECONDS);
            if (request.runAt().isAfter(latest)) {
                throw new InvalidJobException("runAt must be at most " + EnqueueRequest.MAX_DELAY_SECONDS
                        + " seconds (30 days) in the future");
            }
        }
        int delaySeconds = request.delaySeconds() == null ? 0 : request.delaySeconds();
        return new NewJob(request.queue(), request.type(), serializePayload(request.payload()),
                request.priority(), request.maxAttempts(), request.timeoutSeconds(), request.idempotencyKey(),
                delaySeconds, request.runAt());
    }

    private JobResponse toResponse(Job job) {
        return JobResponse.from(job, objectMapper.readTree(job.payload()));
    }

    private String serializePayload(JsonNode payload) {
        if (payload == null || payload.isNull()) {
            return "{}";
        }
        if (!payload.isObject()) {
            throw new InvalidJobException("payload must be a JSON object");
        }
        String json = objectMapper.writeValueAsString(payload);
        if (json.getBytes(StandardCharsets.UTF_8).length > MAX_PAYLOAD_BYTES) {
            throw new InvalidJobException("payload must be at most " + MAX_PAYLOAD_BYTES + " bytes");
        }
        return json;
    }
}
