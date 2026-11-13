package dev.jobqueue.server.job;

import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

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

    public JobResponse enqueue(EnqueueRequest request) {
        String payloadJson = serializePayload(request.payload());
        NewJob newJob = new NewJob(request.queue(), request.type(), payloadJson, request.priority(),
                request.maxAttempts(), request.timeoutSeconds(), null);
        UUID id = repository.insert(newJob);
        return get(id);
    }

    public JobResponse get(UUID id) {
        Job job = repository.findById(id).orElseThrow(() -> new JobNotFoundException(id));
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
