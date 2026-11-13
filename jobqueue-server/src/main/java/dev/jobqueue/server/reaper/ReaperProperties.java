package dev.jobqueue.server.reaper;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Reaper settings, bound from {@code jobqueue.reaper.*}. Set {@code jobqueue.reaper.enabled=false} to switch
 * the reaper off on an instance (it is on by default, on every server instance).
 *
 * @param interval  pause between two reaper runs
 * @param batchSize most jobs recovered by one statement; a run repeats until a batch comes back not full
 */
@ConfigurationProperties("jobqueue.reaper")
public record ReaperProperties(
        @DefaultValue("10s") Duration interval,
        @DefaultValue("100") int batchSize) {

    public ReaperProperties {
        if (interval.isNegative() || interval.isZero()) {
            throw new IllegalArgumentException("jobqueue.reaper.interval must be positive");
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException("jobqueue.reaper.batch-size must be at least 1");
        }
    }
}
