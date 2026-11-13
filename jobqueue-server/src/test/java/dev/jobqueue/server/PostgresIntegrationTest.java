package dev.jobqueue.server;

import org.flywaydb.core.Flyway;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;

/**
 * Base class for integration tests that need a real PostgreSQL (H2 has no SKIP LOCKED semantics).
 *
 * <p>The container is static and started once per test JVM, not once per class: starting Postgres takes
 * seconds, and tests isolate themselves by cleaning the tables they use. Testcontainers' Ryuk sidecar
 * removes the container when the JVM exits, so there is no explicit stop.
 */
public abstract class PostgresIntegrationTest {

    // protected so Spring-based tests can point the application's datasource at this container.
    protected static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17");

    protected static final DataSource DATA_SOURCE;
    protected static final JdbcClient JDBC;
    protected static final JdbcTemplate JDBC_TEMPLATE;

    static {
        POSTGRES.start();
        DATA_SOURCE = new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        // The same migrations the server applies at startup, so tests see the real schema.
        Flyway.configure().dataSource(DATA_SOURCE).locations("classpath:db/migration").load().migrate();
        JDBC = JdbcClient.create(DATA_SOURCE);
        JDBC_TEMPLATE = new JdbcTemplate(DATA_SOURCE);
    }
}
