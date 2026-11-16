package dev.jobqueue.server.job;

import dev.jobqueue.common.JobStatus;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.URI;
import java.util.UUID;

/**
 * HTTP layer only: parses and validates the request, calls {@link JobService}, shapes the response.
 */
@RestController
@Tag(name = "Jobs", description = "Create, inspect and control jobs")
public class JobController {

    /** The most items one page may hold: a larger page would make a single request expensive. */
    static final int MAX_PAGE_SIZE = 100;

    private final JobService service;

    public JobController(JobService service) {
        this.service = service;
    }

    /**
     * 201 when a job was created, 200 when the idempotency key matched an existing job (same body either way).
     */
    @PostMapping("/jobs")
    @Operation(summary = "Enqueue a job",
            description = "201 for a new job; 200 with the existing job when the idempotencyKey was already used "
                    + "in this queue. Use delaySeconds or runAt (not both) to schedule it for later.")
    public ResponseEntity<JobResponse> enqueue(@Valid @RequestBody EnqueueRequest request) {
        EnqueueResult result = service.enqueue(request);
        URI location = ServletUriComponentsBuilder.fromCurrentRequest()
                .path("/{id}").buildAndExpand(result.job().id()).toUri();
        HttpStatus status = result.created() ? HttpStatus.CREATED : HttpStatus.OK;
        return ResponseEntity.status(status).location(location).body(result.job());
    }

    @PostMapping("/jobs/batch")
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(summary = "Enqueue up to 1000 jobs at once, all or nothing")
    public BatchEnqueueResponse enqueueBatch(@Valid @RequestBody BatchEnqueueRequest request) {
        return service.enqueueBatch(request);
    }

    @GetMapping("/jobs")
    @Operation(summary = "List jobs, newest first",
            description = "Optional filters: queue, status, type. Paged: page starts at 0, size is 1 to "
                    + MAX_PAGE_SIZE + ".")
    public PageResponse<JobResponse> list(
            @Parameter(description = "Only jobs of this queue") @RequestParam(required = false) String queue,
            @Parameter(description = "Only jobs with this status") @RequestParam(required = false) JobStatus status,
            @Parameter(description = "Only jobs of this type") @RequestParam(required = false) String type,
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "20") @Min(1) @Max(MAX_PAGE_SIZE) int size) {
        return service.list(queue, status, type, page, size);
    }

    @GetMapping("/jobs/{id}")
    @Operation(summary = "A job with the history of its attempts")
    public JobDetailResponse get(@PathVariable UUID id) {
        return service.getDetail(id);
    }

    @PostMapping("/jobs/{id}/cancel")
    @Operation(summary = "Cancel a job that is still QUEUED",
            description = "409 if the job is RUNNING (it cannot be stopped from outside) or already finished.")
    public JobResponse cancel(@PathVariable UUID id) {
        return service.cancel(id);
    }

    @PostMapping("/jobs/{id}/retry")
    @Operation(summary = "Put a DEAD job back in the queue",
            description = "The job gets a fresh set of attempts and is runnable at once. 409 if it is not DEAD.")
    public JobResponse retry(@PathVariable UUID id) {
        return service.retry(id);
    }
}
