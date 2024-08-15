package works.bosk.drivers.postgresql;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import works.bosk.drivers.DriverConformanceTest;
import works.bosk.drivers.state.TestEntity;
import works.bosk.jackson.JacksonPlugin;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;

class PostgresDriverConformanceTest extends DriverConformanceTest {
	static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(DockerImageName.parse("postgres:latest"));
	public static final String DATABASE = "bosk_test_database";
	private final Deque<Runnable> tearDownActions = new ArrayDeque<>();

	final AtomicInteger schemaCounter = new AtomicInteger(0);

	@BeforeAll
	static void setupDatabase() {
		try (
			var connection = getConnection();
			var stmt = connection.createStatement()
		) {
			stmt.execute("CREATE DATABASE " + quoted(DATABASE));
		} catch (SQLException e) {
			throw new AssertionError("Unexpected error setting up database", e);
		}
	}

	@AfterAll
	static void cleanupDatabase() {
		try (
			var connection = getConnection();
			var stmt = connection.createStatement()
		) {
			stmt.execute("DROP DATABASE IF EXISTS " + quoted(DATABASE));
		} catch (SQLException e) {
			throw new AssertionError("Unexpected error cleaning up database", e);
		}
	}

	@BeforeEach
	void setupDriverFactory() {
		driverFactory = (boskInfo, downstream) -> {
			PostgresDriverSettings settings = new PostgresDriverSettings(
				"jdbc:tc:postgresql://localhost:5432",
				DATABASE,
				"bosk_test_schema_" + schemaCounter.incrementAndGet()
			);
			tearDownActions.addFirst(() -> cleanupSchema(settings));
			return PostgresDriver.<TestEntity>factory(
				settings,
				b -> new ObjectMapper()
					.enable(INDENT_OUTPUT)
					.registerModule(new JacksonPlugin().moduleFor(b))
			).build(boskInfo, downstream);
		};
	}

	@AfterEach
	void runTearDown() {
		tearDownActions.forEach(Runnable::run);
	}

	private void cleanupSchema(PostgresDriverSettings settings) {
		try (
			var connection = getConnection();
			var stmt = connection.createStatement()
		) {
			stmt.execute("DROP SCHEMA IF EXISTS " + quoted(settings.schema()));
		} catch (SQLException e) {
			throw new AssertionError("Unexpected error cleaning up schema", e);
		}
	}

	private static Connection getConnection() throws SQLException {
		return DriverManager.getConnection("jdbc:tc:postgresql://localhost:5432", new Properties());
	}

	private static String quoted(String raw) {
		return '"' + raw.replace("\"", "\"\"") + '"';
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(PostgresDriverConformanceTest.class);
}
