package dev.jobqueue.worker.heartbeat;

import dev.jobqueue.worker.WorkerProperties;
import dev.jobqueue.worker.claim.JobClaimRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Keeps the leases of this worker's running jobs alive, and finds out which jobs it has lost.
 *
 * <p>Invariant: a job whose lease could not be extended is no longer ours, so its handler is interrupted and
 * the worker never writes a result for it (every write is fenced anyway; this just stops wasted work).
 */
@Component
public class HeartbeatService implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatService.class);

    private final JobClaimRepository claimRepository;
    private final RunningJobs runningJobs;
    private final WorkerProperties properties;

    private ScheduledExecutorService scheduler;

    public HeartbeatService(JobClaimRepository claimRepository, RunningJobs runningJobs,
            WorkerProperties properties) {
        this.claimRepository = claimRepository;
        this.runningJobs = runningJobs;
        this.properties = properties;
    }

    @Override
    public synchronized void start() {
        if (scheduler != null) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(task ->
                Thread.ofPlatform().name("heartbeat-" + properties.workerId()).daemon().unstarted(task));
        long intervalMillis = properties.heartbeatInterval().toMillis();
        scheduler.scheduleWithFixedDelay(this::beat, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
        log.info("Heartbeat every {} for a lease of {}", properties.heartbeatInterval(), properties.lease());
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
     * One heartbeat: extend every running job's lease, then cancel the handlers of the jobs that were lost.
     * Never throws: with scheduleWithFixedDelay an exception would silently cancel all later heartbeats.
     */
    void beat() {
        try {
            List<RunningJobs.Entry> running = runningJobs.snapshot();
            if (running.isEmpty()) {
                return;
            }
            Map<UUID, Integer> attemptsById = new HashMap<>();
            running.forEach(entry -> attemptsById.put(entry.job().id(), entry.job().attempt()));

            Set<UUID> kept = claimRepository.extendLeases(properties.workerId(), attemptsById, properties.lease());

            for (RunningJobs.Entry entry : running) {
                // A handler that has already finished is not "lost": its job was just completed or failed,
                // so it is no longer RUNNING and naturally missing from the result.
                if (!kept.contains(entry.job().id()) && !entry.handler().isDone()) {
                    log.warn("Lease lost for job {} ({}), attempt {}: interrupting its handler",
                            entry.job().id(), entry.job().type(), entry.job().attempt());
                    entry.handler().cancel(true);
                }
            }
        } catch (RuntimeException e) {
            // Usually a database hiccup. Jobs are not given up on here: if the outage outlasts the lease,
            // the reaper recovers them and fencing rejects whatever this worker writes afterwards.
            log.error("Heartbeat failed, will retry at the next interval", e);
        }
    }
}
