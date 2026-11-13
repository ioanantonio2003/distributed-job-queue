package dev.jobqueue.server.job;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.URI;
import java.util.UUID;

/**
 * HTTP layer only: parses and validates the request, calls {@link JobService}, shapes the response.
 */
@RestController
public class JobController {

    private final JobService service;

    public JobController(JobService service) {
        this.service = service;
    }

    /**
     * 201 when a job was created, 200 when the idempotency key matched an existing job (same body either way).
     */
    @PostMapping("/jobs")
    public ResponseEntity<JobResponse> enqueue(@Valid @RequestBody EnqueueRequest request) {
        EnqueueResult result = service.enqueue(request);
        URI location = ServletUriComponentsBuilder.fromCurrentRequest()
                .path("/{id}").buildAndExpand(result.job().id()).toUri();
        HttpStatus status = result.created() ? HttpStatus.CREATED : HttpStatus.OK;
        return ResponseEntity.status(status).location(location).body(result.job());
    }

    @PostMapping("/jobs/batch")
    @ResponseStatus(HttpStatus.CREATED)
    public BatchEnqueueResponse enqueueBatch(@Valid @RequestBody BatchEnqueueRequest request) {
        return service.enqueueBatch(request);
    }

    @GetMapping("/jobs/{id}")
    public JobResponse get(@PathVariable UUID id) {
        return service.get(id);
    }
}
