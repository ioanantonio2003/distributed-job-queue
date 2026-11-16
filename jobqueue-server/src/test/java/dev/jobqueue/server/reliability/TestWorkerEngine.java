package dev.jobqueue.server.reliability;

import com.zaxxer.hikari.HikariDataSource;
import dev.jobqueue.worker.WorkerProperties;
import dev.jobqueue.worker.claim.JobClaimRepository;
import dev.jobqueue.worker.completion.JobCompletionRepository;
import dev.jobqueue.worker.handler.HandlerRegistry;
import dev.jobqueue.worker.heartbeat.HeartbeatService;
import dev.jobqueue.worker.heartbeat.RunningJobs;
import dev.jobqueue.worker.poller.JobPoller;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * One simulated worker process: a poller and (optionally) a heartbeat, with their own connection pool.
 *
 * <p>The pool is what makes {@link #crash()} a believable crash: closing it cuts every database access this
 * "process" has, so nothing it was doing can reach the database any more, exactly as if the process had died.
 */
final class TestWorkerEngine {

    private static final Duration HEARTBEAT = Duration.ofSeconds(1);

    private final String workerId;
    private final HikariDataSource pool;
    private final AnnotationConfigApplicationContext spring = new AnnotationConfigApplicationContext();
    private final RunningJobs runningJobs = new RunningJobs();
    private final JobPoller poller;
    private final HeartbeatService heartbeat; // null for an engine whose heartbeat is dead from the start
    private final List<Boolean> completionResults = new CopyOnWriteArrayList<>();

    /**
     * @param lease            lease of the jobs it claims; the heartbeat interval is a third of it
     * @param heartbeatEnabled false builds a worker that never renews its leases (a frozen one)
     * @param handlers         beans with {@code @JobHandler} methods; the same instance can be shared by engines
     */
    @SuppressWarnings("unchecked")
    TestWorkerEngine(String workerId, String queue, int slots, Duration lease, boolean heartbeatEnabled,
            HikariDataSource pool, Object... handlers) {
        this.workerId = workerId;
        this.pool = pool;
        JdbcClient jdbc = JdbcClient.create(pool);

        spring.registerBean(ObjectMapper.class, () -> JsonMapper.builder().build());
        for (int i = 0; i < handlers.length; i++) {
            Object handler = handlers[i];
            spring.registerBean("handler" + i, (Class<Object>) handler.getClass(), () -> handler);
        }
        spring.register(HandlerRegistry.class);
        spring.refresh();

        WorkerProperties properties = new WorkerProperties(workerId, List.of(queue), slots, lease,
                Duration.ofMillis(20), Duration.ofMillis(100), Duration.ofMillis(100), Duration.ofSeconds(1),
                lease.dividedBy(3), null, Duration.ZERO);
        JobClaimRepository claimRepository = new JobClaimRepository(jdbc);
        // Remembers what happened to every completion this worker attempted, to show a zombie being rejected.
        JobCompletionRepository completionRepository = new JobCompletionRepository(jdbc) {
            @Override
            public boolean complete(UUID jobId, String workerId, int attempt) {
                boolean accepted = super.complete(jobId, workerId, attempt);
                completionResults.add(accepted);
                return accepted;
            }
        };
        poller = new JobPoller(claimRepository, completionRepository, spring.getBean(HandlerRegistry.class),
                properties, runningJobs);
        heartbeat = heartbeatEnabled ? new HeartbeatService(claimRepository, runningJobs, properties) : null;
    }

    String workerId() {
        return workerId;
    }

    /** The result of every completion this worker attempted: true = accepted, false = fenced out. */
    List<Boolean> completionResults() {
        return completionResults;
    }

    void start() {
        poller.start();
        if (heartbeat != null) {
            heartbeat.start();
        }
    }

    /**
     * Simulates the sudden death of the process: no completion, no failure report, no more heartbeats.
     * The pool is closed first, so that nothing still running can write to the database afterwards.
     */
    void crash() {
        pool.close();
        if (heartbeat != null) {
            heartbeat.stop();
        }
        poller.stop();
        runningJobs.snapshot().forEach(entry -> entry.handler().cancel(true)); // the dead process runs nothing
        spring.close();
    }

    /** A clean stop at the end of a test; safe after {@link #crash()}. */
    void shutdown() {
        crash();
    }
}
