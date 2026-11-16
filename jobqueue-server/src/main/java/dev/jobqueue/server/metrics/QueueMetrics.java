package dev.jobqueue.server.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Publishes how full the queues are, as gauges refreshed from the database every few seconds:
 * {@code jobqueue.queue.jobs{queue,state}} (ready, scheduled, running) and
 * {@code jobqueue.queue.oldest.age.seconds{queue}} (how long the longest-waiting ready job has waited).
 *
 * <p>A gauge is a value that goes up and down; the database is the only place that knows the queue depth,
 * so a periodic query is the source. The query reads only QUEUED and RUNNING jobs, which the partial indexes
 * cover, so it stays cheap however many finished jobs the table holds.
 */
@Component
public class QueueMetrics {

    private static final Logger log = LoggerFactory.getLogger(QueueMetrics.class);

    private record QueueRow(String queue, long ready, long scheduled, long running, double oldestAgeSeconds) {
    }

    private final JdbcClient jdbc;
    private final MultiGauge jobs;
    private final MultiGauge oldestAge;
    // A queue that has been seen keeps being reported (as zeros) when it empties, so graphs show 0, not a gap.
    private final Set<String> knownQueues = ConcurrentHashMap.newKeySet();

    public QueueMetrics(JdbcClient jdbc, MeterRegistry registry) {
        this.jdbc = jdbc;
        this.jobs = MultiGauge.builder("jobqueue.queue.jobs")
                .description("Jobs per queue: ready to run, scheduled for later, or running")
                .register(registry);
        this.oldestAge = MultiGauge.builder("jobqueue.queue.oldest.age.seconds")
                .description("How long the longest-waiting ready job has waited, in seconds (0 if none waits)")
                .register(registry);
    }

    @Scheduled(fixedDelayString = "${jobqueue.metrics.refresh-interval:5s}")
    public void refresh() {
        try {
            Map<String, QueueRow> byQueue = new ConcurrentHashMap<>();
            for (QueueRow row : readQueues()) {
                byQueue.put(row.queue(), row);
                knownQueues.add(row.queue());
            }
            List<MultiGauge.Row<?>> jobRows = new ArrayList<>();
            List<MultiGauge.Row<?>> ageRows = new ArrayList<>();
            for (String queue : knownQueues) {
                QueueRow row = byQueue.getOrDefault(queue, new QueueRow(queue, 0, 0, 0, 0));
                jobRows.add(MultiGauge.Row.of(Tags.of("queue", queue, "state", "ready"), row.ready()));
                jobRows.add(MultiGauge.Row.of(Tags.of("queue", queue, "state", "scheduled"), row.scheduled()));
                jobRows.add(MultiGauge.Row.of(Tags.of("queue", queue, "state", "running"), row.running()));
                ageRows.add(MultiGauge.Row.of(Tags.of("queue", queue), row.oldestAgeSeconds()));
            }
            jobs.register(jobRows, true);
            oldestAge.register(ageRows, true);
        } catch (RuntimeException e) {
            // A database hiccup must not break the schedule; the gauges keep their last values until the next run.
            log.warn("Could not refresh the queue metrics: {}", e.toString());
        }
    }

    private List<QueueRow> readQueues() {
        return jdbc.sql("""
                        SELECT queue,
                               count(*) FILTER (WHERE status = 'QUEUED' AND run_at <= now()) AS ready,
                               count(*) FILTER (WHERE status = 'QUEUED' AND run_at >  now()) AS scheduled,
                               count(*) FILTER (WHERE status = 'RUNNING')                    AS running,
                               coalesce(CAST(EXTRACT(EPOCH FROM now() - min(run_at)
                                    FILTER (WHERE status = 'QUEUED' AND run_at <= now())) AS double precision), 0)
                                                                                             AS oldest_age
                        FROM jobs
                        WHERE status IN ('QUEUED', 'RUNNING')
                        GROUP BY queue
                        """)
                .query((rs, n) -> new QueueRow(rs.getString("queue"), rs.getLong("ready"),
                        rs.getLong("scheduled"), rs.getLong("running"), rs.getDouble("oldest_age")))
                .list();
    }
}
