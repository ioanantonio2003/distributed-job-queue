package dev.jobqueue.server.job;

/**
 * Jobs of one queue by status. {@code oldestQueuedAgeSeconds} is how long the job that has waited longest has
 * been runnable (counted from its run_at, so a job scheduled for later does not count), or null when no job
 * is waiting.
 */
public record QueueStats(
        String queue,
        long queued,
        long running,
        long succeeded,
        long dead,
        long cancelled,
        Double oldestQueuedAgeSeconds) {
}
