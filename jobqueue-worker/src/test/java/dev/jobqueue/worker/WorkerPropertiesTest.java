package dev.jobqueue.worker;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerPropertiesTest {

    private static WorkerProperties with(String workerId, int slots, Duration poll, Duration maxPoll) {
        return new WorkerProperties(workerId, List.of("default"), slots, Duration.ofSeconds(30), poll, maxPoll,
                Duration.ofSeconds(2), Duration.ofMinutes(5), null, null, null);
    }

    @Test
    void shouldRejectRetryMaxDelayBelowRetryBaseDelay() {
        assertThatThrownBy(() -> new WorkerProperties("w", List.of("default"), 4, Duration.ofSeconds(30),
                Duration.ofMillis(500), Duration.ofSeconds(5), Duration.ofMinutes(1), Duration.ofSeconds(1), null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retry-base-delay");
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

    private static WorkerProperties withQueues(List<String> queues, int slots, Map<String, Integer> concurrency) {
        return new WorkerProperties("w", queues, slots, Duration.ofSeconds(30), Duration.ofMillis(500),
                Duration.ofSeconds(5), Duration.ofSeconds(2), Duration.ofMinutes(5), null, concurrency, null);
    }

    @Test
    void shouldServeTheDefaultQueueWhenNothingIsConfigured() {
        assertThat(withQueues(List.of(), 4, null).queueSlots()).containsExactly(Map.entry("default", 4));
    }

    @Test
    void shouldGiveEveryListedQueueTheDefaultSlotsWhenNoConcurrencyIsSet() {
        assertThat(withQueues(List.of("a", "b"), 3, null).queueSlots())
                .containsExactly(Map.entry("a", 3), Map.entry("b", 3));
    }

    @Test
    void shouldLetConcurrencyOverrideTheSlotsOfOneQueue() {
        assertThat(withQueues(List.of("a", "b"), 4, Map.of("b", 1)).queueSlots())
                .containsExactly(Map.entry("a", 4), Map.entry("b", 1));
    }

    @Test
    void shouldServeAQueueNamedOnlyInConcurrencyAndNotTheDefaultOne() {
        // Only emails and reports: "default" is NOT served just because it is the fallback.
        assertThat(withQueues(List.of(), 4, Map.of("emails", 4, "reports", 2)).queueSlots())
                .containsOnlyKeys("emails", "reports")
                .containsEntry("emails", 4).containsEntry("reports", 2);
    }

    @Test
    void shouldRejectConcurrencyBelowOneAndBlankQueueNames() {
        assertThatThrownBy(() -> withQueues(List.of(), 4, Map.of("a", 0)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("concurrency");
        assertThatThrownBy(() -> withQueues(List.of(" "), 4, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("blank");
    }

    private static WorkerProperties withLeaseAndHeartbeat(Duration lease, Duration heartbeat) {
        return new WorkerProperties("w", List.of("default"), 4, lease, Duration.ofMillis(500), Duration.ofSeconds(5),
                Duration.ofSeconds(2), Duration.ofMinutes(5), heartbeat, null, null);
    }

    @Test
    void shouldDefaultHeartbeatIntervalToAThirdOfTheLeaseWhenNotConfigured() {
        assertThat(withLeaseAndHeartbeat(Duration.ofSeconds(30), null).heartbeatInterval())
                .isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void shouldAcceptALeaseOfExactlyThreeHeartbeats() {
        assertThat(withLeaseAndHeartbeat(Duration.ofSeconds(30), Duration.ofSeconds(10)).heartbeatInterval())
                .isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void shouldRefuseToStartWhenLeaseIsShorterThanThreeHeartbeats() {
        assertThatThrownBy(() -> withLeaseAndHeartbeat(Duration.ofSeconds(29), Duration.ofSeconds(10)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 3 x")
                .hasMessageContaining("heartbeat-interval");
    }

    @Test
    void shouldRejectNonPositiveHeartbeatInterval() {
        assertThatThrownBy(() -> withLeaseAndHeartbeat(Duration.ofSeconds(30), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("heartbeat-interval must be positive");
    }
}
