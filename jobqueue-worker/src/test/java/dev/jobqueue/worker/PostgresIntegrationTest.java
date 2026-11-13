package dev.jobqueue.worker;

import org.flywaydb.core.Flyway;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;

/**
 * Base class for integration tests that need a real PostgreSQL (H2 has no SKIP LOCKED semantics).
 * Same idea as the server's copy: one container per test JVM, cleaned up by Testcontainers' Ryuk sidecar.
 */
public abstract class PostgresIntegrationTest {

    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17");

    protected static final DataSource DATA_SOURCE;
    protected static final JdbcClient JDBC;

    static {
        POSTGRES.start();
        DATA_SOURCE = new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        // The same migrations the server applies at startup, so tests see the real schema.
        Flyway.configure().dataSource(DATA_SOURCE).locations("classpath:db/migration").load().migrate();
        JDBC = JdbcClient.create(DATA_SOURCE);
    }
}
