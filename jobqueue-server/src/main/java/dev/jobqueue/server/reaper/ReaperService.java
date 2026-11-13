package dev.jobqueue.server.reaper;

import dev.jobqueue.server.reaper.ReaperRepository.ReapResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runs the reaper periodically. Every server instance runs one; they do not coordinate, because
 * {@link ReaperRepository} is safe to run concurrently.
 */
@Component
@ConditionalOnProperty(prefix = "jobqueue.reaper", name = "enabled", havingValue = "true", matchIfMissing = true)
public class ReaperService implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ReaperService.class);

    private final ReaperRepository repository;
    private final ReaperProperties properties;

    private ScheduledExecutorService scheduler;

    public ReaperService(ReaperRepository repository, ReaperProperties properties) {
        this.repository = repository;
        this.properties = properties;
    }

    @Override
    public synchronized void start() {
        if (scheduler != null) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(task ->
                Thread.ofPlatform().name("reaper").daemon().unstarted(task));
        long intervalMillis = properties.interval().toMillis();
        scheduler.scheduleWithFixedDelay(this::runSafely, 0, intervalMillis, TimeUnit.MILLISECONDS);
        log.info("Reaper started: every {}, batch size {}", properties.interval(), properties.batchSize());
    }

    @Override
    public synchronized void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    @Override
    public synchronized boolean isRunning() {
        return scheduler != null;
    }

    /**
     * Recovers every job whose lease has expired, batch after batch, until a batch comes back not full.
     */
    public ReapResult reapAll() {
        ReapResult total = ReapResult.NONE;
        ReapResult batch;
        do {
            batch = repository.reap(properties.batchSize());
            total = total.plus(batch);
        } while (batch.total() == properties.batchSize());
        return total;
    }

    // Never throws: with scheduleWithFixedDelay an exception would silently cancel every later run.
    private void runSafely() {
        try {
            ReapResult result = reapAll();
            if (result.total() > 0) {
                log.warn("Reaped {} job(s) with an expired lease: {} requeued, {} dead",
                        result.total(), result.requeued(), result.dead());
            }
        } catch (RuntimeException e) {
            log.error("Reaper run failed, will retry at the next interval", e);
        }
    }
}
