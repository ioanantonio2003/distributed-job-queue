package dev.jobqueue.worker.heartbeat;

import dev.jobqueue.worker.claim.ClaimedJob;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * The jobs this worker is executing right now, with a handle on each handler.
 *
 * <p>The poller adds a job the moment it claims it and removes it when its execution is over; the heartbeat reads
 * a snapshot to know whose lease to extend, and cancels the handlers of jobs it finds lost; the shutdown waits
 * for it to become empty. Entries are written by many threads and read by others, hence the concurrent map.
 */
@Component
public class RunningJobs {

    public record Entry(ClaimedJob job, Future<?> handler) {
    }

    private final ConcurrentHashMap<UUID, Entry> running = new ConcurrentHashMap<>();
    private final Object emptied = new Object();

    public void add(ClaimedJob job, Future<?> handler) {
        running.put(job.id(), new Entry(job, handler));
    }

    public void remove(UUID jobId) {
        running.remove(jobId);
        synchronized (emptied) {
            emptied.notifyAll(); // someone may be waiting for the last job to finish
        }
    }

    public List<Entry> snapshot() {
        return List.copyOf(running.values());
    }

    /**
     * Waits until no job is running, for at most {@code timeout}.
     *
     * @return true if it became empty, false if the time ran out first
     */
    public boolean awaitEmpty(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (emptied) {
            while (!running.isEmpty()) {
                long left = deadline - System.nanoTime();
                if (left <= 0) {
                    return false;
                }
                TimeUnit.NANOSECONDS.timedWait(emptied, left);
            }
        }
        return true;
    }
}
