package dev.jobqueue.worker.poller;

import dev.jobqueue.common.AttemptOutcome;
import dev.jobqueue.common.BackoffCalculator;
import dev.jobqueue.common.JobContext;
import dev.jobqueue.common.JobStatus;
import dev.jobqueue.common.PermanentFailureException;
import dev.jobqueue.worker.WorkerProperties;
import dev.jobqueue.worker.claim.ClaimedJob;
import dev.jobqueue.worker.claim.JobClaimRepository;
import dev.jobqueue.worker.completion.JobCompletionRepository;
import dev.jobqueue.worker.handler.HandlerRegistry;
import dev.jobqueue.worker.heartbeat.RunningJobs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Claims jobs and runs their handlers, never more at once <b>per queue</b> than that queue's slots.
 *
 * <p>Invariants: (1) the number of jobs being executed from a queue never exceeds that queue's slot count,
 * because a permit of the queue is taken before a job is claimed and returned only when its execution ends, by
 * completion, failure or timeout; (2) a handler runs on its own virtual thread, after the claim statement has
 * finished, so it holds no database connection or transaction; (3) queues do not share permits, so a queue
 * whose jobs are slow can fill its own slots and nothing else (the bulkhead). Caveat to (1): after a timeout the
 * slot is freed even if the handler ignored the interrupt and is still running in the background (Java cannot
 * kill a thread).
 *
 * <p>Only the poller thread takes permits; handler threads only give them back. That is what makes
 * "take everything that is free, then claim that many" safe without extra locking.
 */
@Component
public class JobPoller implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(JobPoller.class);

    private final JobClaimRepository claimRepository;
    private final JobCompletionRepository completionRepository;
    private final HandlerRegistry registry;
    private final WorkerProperties properties;
    private final RunningJobs runningJobs;
    private final BackoffCalculator backoff;
    private final List<QueueSlots> queues;
    // Released by every finishing handler, to wake the poller up as soon as a slot is free again.
    private final Semaphore wakeups = new Semaphore(0);

    /** One queue and its own permits: the unit of isolation. */
    private record QueueSlots(String name, Semaphore permits) {
    }

    private volatile boolean running;
    // Set when a shutdown begins; only changes how an interrupted handler is logged.
    private volatile boolean shuttingDown;
    private Thread pollerThread;

    public JobPoller(JobClaimRepository claimRepository, JobCompletionRepository completionRepository,
            HandlerRegistry registry, WorkerProperties properties, RunningJobs runningJobs) {
        this.claimRepository = claimRepository;
        this.completionRepository = completionRepository;
        this.registry = registry;
        this.properties = properties;
        this.runningJobs = runningJobs;
        this.backoff = new BackoffCalculator(properties.retryBaseDelay(), properties.retryMaxDelay());
        this.queues = properties.queueSlots().entrySet().stream()
                .map(entry -> new QueueSlots(entry.getKey(), new Semaphore(entry.getValue())))
                .toList();
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        // A dedicated platform thread: it spends its life blocked on the semaphore, the database or a sleep.
        pollerThread = Thread.ofPlatform().name("job-poller-" + properties.workerId()).start(this::pollLoop);
        log.info("Worker {} started: queues (name=slots)={}, lease={}",
                properties.workerId(), properties.queueSlots(), properties.lease());
    }

    /**
     * Graceful shutdown, in two steps. DRAINING: stop claiming and let the running jobs finish, for at most the
     * shutdown grace. Then, for whatever is still unfinished: interrupt its handler and give the job back to the
     * queue without costing it an attempt.
     *
     * <p>Blocks until both steps are done (Spring waits for it, up to spring.lifecycle.timeout-per-shutdown-phase).
     */
    @Override
    public synchronized void stop() {
        if (pollerThread == null) {
            return; // never started, or already stopped
        }
        shuttingDown = true;
        running = false; // DRAINING: the poll loop claims nothing more
        pollerThread.interrupt();
        try {
            pollerThread.join(5_000); // after this, nobody can claim a job any more, so the set below only shrinks
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        pollerThread = null;
        drain();
    }

    private void drain() {
        try {
            if (!runningJobs.snapshot().isEmpty()) {
                log.info("Draining: waiting up to {} for {} running job(s) to finish",
                        properties.shutdownGrace(), runningJobs.snapshot().size());
            }
            if (runningJobs.awaitEmpty(properties.shutdownGrace())) {
                log.info("Drained: every running job finished");
                return;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // stop waiting; give back what is left right away
        }

        // The grace period is over. Take the unfinished jobs off the books first (so the heartbeat stops caring
        // about them), then interrupt their handlers, then give the jobs back to the queue.
        List<RunningJobs.Entry> unfinished = runningJobs.snapshot();
        Map<UUID, Integer> attemptsById = new HashMap<>();
        for (RunningJobs.Entry entry : unfinished) {
            attemptsById.put(entry.job().id(), entry.job().attempt());
            runningJobs.remove(entry.job().id());
        }
        unfinished.forEach(entry -> entry.handler().cancel(true));
        try {
            int released = completionRepository.release(properties.workerId(), attemptsById);
            log.warn("Grace period over: {} of {} unfinished job(s) given back to the queue, without using an attempt",
                    released, unfinished.size());
        } catch (RuntimeException e) {
            // Not fatal: the leases run out and the reaper recovers the jobs (those do cost an attempt).
            log.error("Could not give {} unfinished job(s) back to the queue; the reaper will recover them",
                    unfinished.size(), e);
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * Stops first, so that the heartbeat (which has a lower phase) is still alive while this one drains
     * and the leases of the jobs still running keep being extended. Spring stops higher phases first.
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    private void pollLoop() {
        Duration idleWait = properties.pollInterval();
        while (running) {
            boolean claimedAny = false;
            boolean anySlotFree = false;

            // One pass over the queues. Each queue is claimed on its own, with its own free slots, so a
            // queue that is full (or slow) is simply skipped and never holds the others back.
            for (QueueSlots queue : queues) {
                int free = queue.permits().drainPermits(); // only this thread takes permits
                if (free == 0) {
                    continue;
                }
                anySlotFree = true;
                List<ClaimedJob> claimed;
                try {
                    // Ask for exactly as many jobs as this queue has free slots (backpressure).
                    claimed = claimRepository.claim(properties.workerId(), queue.name(), properties.lease(), free);
                } catch (RuntimeException e) {
                    log.error("Claim from queue '{}' failed, will retry", queue.name(), e);
                    queue.permits().release(free);
                    continue;
                }
                queue.permits().release(free - claimed.size()); // slots we did not use
                claimed.forEach(job -> startHandler(job, queue));
                claimedAny |= !claimed.isEmpty();
            }

            if (claimedAny) {
                idleWait = properties.pollInterval();
                continue; // there may be more waiting: look again at once
            }
            // Nothing claimed. If every slot is busy there is nothing to look for until a handler ends (its
            // release wakes us up, the long timeout is only a safety net). Otherwise the queues are empty or
            // the database failed: wait, and wait longer each time.
            if (!awaitWakeup(anySlotFree ? idleWait : properties.maxPollInterval())) {
                return;
            }
            if (anySlotFree) {
                idleWait = nextWait(idleWait);
            }
        }
    }

    private void startHandler(ClaimedJob job, QueueSlots queue) {
        JobContext context = new JobContext(job.id(), job.queue(), job.type(), job.attempt(), job.idempotencyKey());
        FutureTask<Void> handler = new FutureTask<>(() -> {
            registry.invoke(context, job.payload());
            return null;
        });
        // Registered here, by the poller thread itself, the moment the job is claimed: from now on the heartbeat
        // extends its lease and a shutdown knows about it, with no gap in which a claimed job is invisible.
        runningJobs.add(job, handler);
        Thread.ofVirtual().name("job-" + job.id()).start(() -> {
            try {
                // The handler runs on a second virtual thread so this one can wait for it with a time limit.
                // If a shutdown cancelled it already, it simply does not run.
                Thread.ofVirtual().name("handler-" + job.id()).start(handler);
                awaitOutcome(job, handler);
            } finally {
                runningJobs.remove(job.id());
                queue.permits().release();
                wakeups.release(); // a slot is free: let the poller look at once, instead of at its next wait
            }
        });
    }

    private void awaitOutcome(ClaimedJob job, FutureTask<Void> handler) {
        try {
            handler.get(job.timeoutSeconds(), TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            // cancel(true) only *asks* the handler thread to stop, by interrupting it. Code that blocks (sleep, I/O,
            // locks) or checks its interrupt flag stops; a busy loop that ignores it keeps running unseen.
            // Either way this job's slot is freed: a stuck handler must not hold it forever.
            handler.cancel(true);
            fail(job, new TimeoutException("handler exceeded timeout of " + job.timeoutSeconds() + " s"),
                    false, AttemptOutcome.TIMED_OUT);
            return;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof PermanentFailureException permanent) {
                fail(job, permanent, true, AttemptOutcome.FAILED);
            } else if (cause instanceof Exception exception) {
                // Everything else is assumed to be transient, including an unknown job type (another worker
                // may have the handler): retry, until the attempts run out.
                fail(job, exception, false, AttemptOutcome.FAILED);
            } else if (cause instanceof Error error) {
                throw error; // an Error is not hidden: this thread dies and the job stays RUNNING until recovered
            } else {
                throw new IllegalStateException(cause);
            }
            return;
        } catch (CancellationException e) {
            // A handler is cancelled from outside in two cases. Either way nothing is written here.
            if (shuttingDown) {
                // The grace period ran out: the shutdown is giving this job back to the queue itself.
                log.info("Job {} ({}) interrupted by the worker shutdown; it is given back to the queue",
                        job.id(), job.type());
            } else {
                // The heartbeat found the job is no longer ours: its new owner (or the reaper) decides.
                log.warn("Job {} ({}) lease lost: attempt {} was cancelled and its result is ignored",
                        job.id(), job.type(), job.attempt());
            }
            return;
        } catch (InterruptedException e) {
            // Not expected (nothing interrupts this thread); leave the job RUNNING for the lease to recover.
            handler.cancel(true);
            Thread.currentThread().interrupt();
            log.warn("Interrupted while waiting for job {}; it is left RUNNING", job.id());
            return;
        }
        complete(job);
    }

    private void fail(ClaimedJob job, Exception cause, boolean permanent, AttemptOutcome outcome) {
        Duration delay = permanent ? Duration.ZERO : backoff.delay(job.attempt());
        try {
            Optional<JobStatus> status = completionRepository.fail(job.id(), properties.workerId(), job.attempt(),
                    describe(cause), permanent, delay, outcome);
            if (status.isEmpty()) {
                log.warn("Job {} ({}) lease lost: failure of attempt {} ignored", job.id(), job.type(), job.attempt());
            } else if (status.get() == JobStatus.DEAD) {
                log.warn("Job {} ({}) is DEAD after attempt {}: {}", job.id(), job.type(), job.attempt(),
                        describe(cause));
            } else {
                log.info("Job {} ({}) failed on attempt {}, retry in {}: {}", job.id(), job.type(), job.attempt(),
                        delay, describe(cause));
            }
        } catch (RuntimeException e) {
            // The job stays RUNNING; when its lease expires it will be recovered (reaper, later step).
            log.error("Could not record failure of job {}", job.id(), e);
        }
    }

    // What goes into jobs.last_error: the exception type and message, truncated so one huge message cannot bloat a row.
    private static String describe(Exception e) {
        String text = e.getClass().getSimpleName() + ": " + e.getMessage();
        return text.length() <= 2000 ? text : text.substring(0, 2000);
    }

    // The handler is already finished here, so this is a separate short statement: no connection or
    // transaction was held while the handler ran.
    private void complete(ClaimedJob job) {
        try {
            if (completionRepository.complete(job.id(), properties.workerId(), job.attempt())) {
                log.info("Job {} ({}) succeeded on attempt {}", job.id(), job.type(), job.attempt());
            } else {
                // Fenced out: another worker owns the job now (or it was already finished). Our result is ignored.
                log.warn("Job {} ({}) lease lost: result of attempt {} ignored", job.id(), job.type(), job.attempt());
            }
        } catch (RuntimeException e) {
            // The job stays RUNNING; when its lease expires it will be recovered (reaper, later step).
            log.error("Could not record completion of job {}", job.id(), e);
        }
    }

    // Wait doubles after each empty poll, up to a cap, so an idle worker does not hammer the database.
    private Duration nextWait(Duration current) {
        Duration doubled = current.multipliedBy(2);
        return doubled.compareTo(properties.maxPollInterval()) > 0 ? properties.maxPollInterval() : doubled;
    }

    // Waits for the timeout, or less if a handler finishes meanwhile. A wake-up that arrived before we started
    // waiting is not lost: the semaphore remembers it. Returns false if the poller was told to stop.
    private boolean awaitWakeup(Duration timeout) {
        try {
            wakeups.tryAcquire(timeout.toMillis(), TimeUnit.MILLISECONDS);
            wakeups.drainPermits(); // one look at the queues covers every wake-up so far
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
