package dev.jobqueue.server.job;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@Tag(name = "Queues", description = "Overview of the queues")
public class QueueController {

    private final JobService service;

    public QueueController(JobService service) {
        this.service = service;
    }

    @GetMapping("/queues")
    @Operation(summary = "Per queue: jobs by status and the age of the oldest waiting job")
    public List<QueueStats> queues() {
        return service.queueStats();
    }
}
