package dev.jobqueue.common;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * How long to wait before retrying a job that has just failed its {@code attempt}-th attempt (1-based).
 *
 * <p>delay = min(cap, base * 2^(attempt - 1)) * U(0.5, 1). The exponential part spaces retries out more and
 * more; the cap stops the wait from growing without bound; the jitter (a random factor between 0.5 and 1)
 * spreads out jobs that failed at the same moment, so they do not all retry together.
 *
 * <p>Invariant: the result is never above {@code cap} and never below half of the capped exponential value.
 */
public final class BackoffCalculator {

    private final Duration base;
    private final Duration cap;
    private final DoubleSupplier random; // returns a value in [0, 1); injectable so tests can control the jitter

    public BackoffCalculator(Duration base, Duration cap) {
        this(base, cap, () -> ThreadLocalRandom.current().nextDouble());
    }

    BackoffCalculator(Duration base, Duration cap, DoubleSupplier random) {
        if (base.isNegative() || base.isZero()) {
            throw new IllegalArgumentException("base must be positive");
        }
        if (cap.compareTo(base) < 0) {
            throw new IllegalArgumentException("cap must be >= base");
        }
        this.base = base;
        this.cap = cap;
        this.random = random;
    }

    public Duration delay(int attempt) {
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt starts at 1");
        }
        // double arithmetic: for a very large attempt 2^n becomes Infinity, and min() then simply picks the cap.
        double exponentialMillis = base.toMillis() * Math.pow(2, attempt - 1);
        double cappedMillis = Math.min(cap.toMillis(), exponentialMillis);
        double jitter = 0.5 + 0.5 * random.getAsDouble();
        return Duration.ofMillis(Math.round(cappedMillis * jitter));
    }
}
