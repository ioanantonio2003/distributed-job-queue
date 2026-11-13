package dev.jobqueue.common;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Random;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BackoffCalculatorTest {

    private static final Duration BASE = Duration.ofSeconds(2);
    private static final Duration CAP = Duration.ofMinutes(5);

    @Test
    void shouldReturnBaseWhenFirstAttemptFailsAndJitterIsAtItsUpperBound() {
        // A random value of 1.0 gives the largest jitter factor (1.0), i.e. the un-reduced delay.
        BackoffCalculator calculator = new BackoffCalculator(BASE, CAP, () -> 1.0);

        assertThat(calculator.delay(1)).isEqualTo(BASE);
    }

    @Test
    void shouldGrowWithTheAttemptNumberWhenJitterIsFixed() {
        BackoffCalculator calculator = new BackoffCalculator(BASE, CAP, () -> 0.5);

        Duration previous = Duration.ZERO;
        for (int attempt = 1; attempt <= 6; attempt++) {
            Duration delay = calculator.delay(attempt);
            assertThat(delay).as("attempt " + attempt).isGreaterThan(previous);
            previous = delay;
        }
    }

    @Test
    void shouldDoubleTheUpperBoundWithEachAttemptWhenBelowTheCap() {
        BackoffCalculator calculator = new BackoffCalculator(BASE, CAP, () -> 1.0);

        assertThat(calculator.delay(1)).isEqualTo(Duration.ofSeconds(2));
        assertThat(calculator.delay(2)).isEqualTo(Duration.ofSeconds(4));
        assertThat(calculator.delay(3)).isEqualTo(Duration.ofSeconds(8));
        assertThat(calculator.delay(4)).isEqualTo(Duration.ofSeconds(16));
    }

    @Test
    void shouldNeverExceedTheCapWhenAttemptsGrowLarge() {
        Random random = new Random(42);
        BackoffCalculator calculator = new BackoffCalculator(BASE, CAP, random::nextDouble);

        for (int attempt = 1; attempt <= 2_000; attempt++) { // 2^2000 overflows a double: must still be capped
            assertThat(calculator.delay(attempt)).as("attempt " + attempt).isLessThanOrEqualTo(CAP);
        }
    }

    @Test
    void shouldReachTheCapWhenAttemptsAreHighEnough() {
        BackoffCalculator calculator = new BackoffCalculator(BASE, CAP, () -> 1.0);

        assertThat(calculator.delay(20)).isEqualTo(CAP);
    }

    @Test
    void shouldKeepJitterBetweenHalfAndFullValueWhenRandomIsUsed() {
        Random random = new Random(7);
        BackoffCalculator calculator = new BackoffCalculator(BASE, CAP, random::nextDouble);

        for (int attempt = 1; attempt <= 10; attempt++) {
            long full = Math.min(CAP.toMillis(), BASE.toMillis() * (1L << (attempt - 1)));
            for (int sample = 0; sample < 1_000; sample++) {
                long millis = calculator.delay(attempt).toMillis();
                assertThat(millis).as("attempt " + attempt).isBetween(full / 2, full);
            }
        }
    }

    @Test
    void shouldProduceDifferentDelaysWhenJitterVaries() {
        BackoffCalculator calculator = new BackoffCalculator(BASE, CAP);

        long distinct = IntStream.range(0, 50).mapToObj(i -> calculator.delay(5)).distinct().count();

        assertThat(distinct).isGreaterThan(1);
    }

    @Test
    void shouldRejectInvalidArguments() {
        assertThatThrownBy(() -> new BackoffCalculator(Duration.ZERO, CAP)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackoffCalculator(CAP, BASE)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackoffCalculator(BASE, CAP).delay(0)).isInstanceOf(IllegalArgumentException.class);
    }
}
