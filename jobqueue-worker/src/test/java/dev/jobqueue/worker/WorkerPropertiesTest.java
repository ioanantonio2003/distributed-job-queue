package dev.jobqueue.worker;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerPropertiesTest {

    private static WorkerProperties with(String workerId, int slots, Duration poll, Duration maxPoll) {
        return new WorkerProperties(workerId, List.of("default"), slots, Duration.ofSeconds(30), poll, maxPoll);
    }

    @Test
    void shouldGenerateDistinctWorkerIdsWhenNoneIsConfigured() {
        WorkerProperties first = with(null, 4, Duration.ofMillis(500), Duration.ofSeconds(5));
        WorkerProperties second = with(" ", 4, Duration.ofMillis(500), Duration.ofSeconds(5));

        assertThat(first.workerId()).isNotBlank().contains("-");
        assertThat(first.workerId()).isNotEqualTo(second.workerId());
    }

    @Test
    void shouldKeepConfiguredWorkerIdWhenProvided() {
        assertThat(with("worker-a", 4, Duration.ofMillis(500), Duration.ofSeconds(5)).workerId())
                .isEqualTo("worker-a");
    }

    @Test
    void shouldRejectZeroSlots() {
        assertThatThrownBy(() -> with("w", 0, Duration.ofMillis(500), Duration.ofSeconds(5)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("slots");
    }

    @Test
    void shouldRejectMaxPollIntervalSmallerThanPollInterval() {
        assertThatThrownBy(() -> with("w", 4, Duration.ofSeconds(5), Duration.ofMillis(500)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max-poll-interval");
    }

    @Test
    void shouldRejectEmptyQueueList() {
        assertThatThrownBy(() -> new WorkerProperties("w", List.of(), 4, Duration.ofSeconds(30),
                Duration.ofMillis(500), Duration.ofSeconds(5)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("queues");
    }
}
