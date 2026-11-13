package dev.jobqueue.worker.heartbeat;

import dev.jobqueue.worker.claim.ClaimedJob;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

/**
 * The jobs this worker is executing right now, with a handle on each handler.
 *
 * <p>The poller adds a job when it starts executing it and removes it when it is done; the heartbeat reads
 * a snapshot to know whose lease to extend, and cancels the handlers of jobs it finds lost. Entries are
 * written by many virtual threads and read by the heartbeat thread, hence the concurrent map.
 */
@Component
public class RunningJobs {

    public record Entry(ClaimedJob job, Future<?> handler) {
    }

    private final ConcurrentHashMap<UUID, Entry> running = new ConcurrentHashMap<>();

    public void add(ClaimedJob job, Future<?> handler) {
        running.put(job.id(), new Entry(job, handler));
    }

    public void remove(UUID jobId) {
        running.remove(jobId);
    }

    public List<Entry> snapshot() {
        return List.copyOf(running.values());
    }
}
