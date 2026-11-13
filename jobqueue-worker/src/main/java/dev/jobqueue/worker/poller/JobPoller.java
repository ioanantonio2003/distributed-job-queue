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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Claims jobs and runs their handlers, never more at once than {@code slots}.
 *
 * <p>Invariants: (1) the number of jobs being executed never exceeds the slot count, because a permit is taken
 * before a job is claimed and returned only when its execution ends, by completion, failure or timeout;
 * (2) a handler runs on its own virtual thread, after the claim statement has finished, so it holds no
 * database connection or transaction. Caveat to (1): after a timeout the slot is freed even if the handler
 * ignored the interrupt and is still running in the background (Java cannot kill a thread).
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
    private final Semaphore slots;

    private volatile boolean running;
    private Thread pollerThread;

    public JobPoller(JobClaimRepository claimRepository, JobCompletionRepository completionRepository,
            HandlerRegistry registry, WorkerProperties properties, RunningJobs runningJobs) {
        this.claimRepository = claimRepository;
        this.completionRepository = completionRepository;
        this.registry = registry;
        this.properties = properties;
        this.runningJobs = runningJobs;
        this.backoff = new BackoffCalculator(properties.retryBaseDelay(), properties.retryMaxDelay());
        this.slots = new Semaphore(properties.slots());
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        // A dedicated platform thread: it spends its life blocked on the semaphore, the database or a sleep.
        pollerThread = Thread.ofPlatform().name("job-poller-" + properties.workerId()).start(this::pollLoop);
        log.info("Worker {} started: queues={}, slots={}, lease={}",
                properties.workerId(), properties.queues(), properties.slots(), properties.lease());
    }

    // Does not wait for running handlers: a graceful drain is a later step.
    @Override
    public synchronized void stop() {
        running = false;
        if (pollerThread != null) {
            pollerThread.interrupt();
            try {
                pollerThread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            pollerThread = null;
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void pollLoop() {
        Duration idleWait = properties.pollInterval();
        while (running) {
            int permits;
            try {
                // Blocks while every slot is busy: no database polling when we could not run anything anyway.
                slots.acquire();
                permits = 1 + slots.drainPermits();
            } catch (InterruptedException e) {
                return;
            }

            List<ClaimedJob> claimed;
            try {
                // Ask for exactly as many jobs as there are free slots (backpressure).
                claimed = claimRepository.claim(properties.workerId(), properties.queues(),
                        properties.lease(), permits);
            } catch (RuntimeException e) {
                log.error("Claim failed, will retry", e);
                slots.release(permits);
                if (!pause(idleWait)) {
                    return;
                }
                idleWait = nextWait(idleWait);
                continue;
            }

            slots.release(permits - claimed.size()); // slots we did not use
            claimed.forEach(this::startHandler);

            if (claimed.isEmpty()) {
                if (!pause(idleWait)) {
                    return;
                }
                idleWait = nextWait(idleWait);
            } else {
                idleWait = properties.pollInterval();
            }
        }
    }

    private void startHandler(ClaimedJob job) {
        Thread.ofVirtual().name("job-" + job.id()).start(() -> {
            try {
                execute(job);
            } finally {
                slots.release();
            }
        });
    }

    private void execute(ClaimedJob job) {
        JobContext context = new JobContext(job.id(), job.queue(), job.type(), job.attempt(), job.idempotencyKey());
        // The handler runs on a second virtual thread so this one can wait for it with a time limit.
        FutureTask<Void> handler = new FutureTask<>(() -> {
            registry.invoke(context, job.payload());
            return null;
        });
        // Visible to the heartbeat (which extends the lease and can cancel the handler) until execution ends.
        runningJobs.add(job, handler);
        try {
            Thread.ofVirtual().name("handler-" + job.id()).start(handler);
            awaitOutcome(job, handler);
        } finally {
            runningJobs.remove(job.id());
        }
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
            // Only the heartbeat cancels a handler from outside: it found the job is no longer ours.
            // Nothing is written; the new owner of the job (or the reaper) decides what happens to it.
            log.warn("Job {} ({}) lease lost: attempt {} was cancelled and its result is ignored",
                    job.id(), job.type(), job.attempt());
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

    private boolean pause(Duration wait) {
        try {
            Thread.sleep(wait);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
