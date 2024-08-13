package works.bosk.drivers.postgresql;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
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
	private final Deque<Runnable> tearDownActions = new ArrayDeque<>();

	final AtomicInteger dbCounter = new AtomicInteger(0);

	@BeforeEach
	void setupDriverFactory() {
		driverFactory = (boskInfo, downstream) -> {
			PostgresDriverSettings settings = new PostgresDriverSettings(
				"jdbc:tc:postgresql://localhost:5432",
				"bosk_test_database_" + dbCounter.incrementAndGet(),
				"bosk_test_schema"
			);
			setupDatabase(settings);
			tearDownActions.addFirst(() -> cleanupDatabase(settings));
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

	private static void setupDatabase(PostgresDriverSettings settings) {
		try (
			var connection = DriverManager.getConnection(settings.url(), new Properties());
			var stmt = connection.createStatement()
		) {
			stmt.execute("CREATE DATABASE " + quoted(settings.database()));
		} catch (SQLException e) {
			throw new AssertionError("Unexpected error setting up database", e);
		}
	}

	private static void cleanupDatabase(PostgresDriverSettings settings) {
		try (
			var connection = DriverManager.getConnection(settings.url(), new Properties());
			var stmt = connection.createStatement()
		) {
			stmt.execute("DROP DATABASE IF EXISTS " + quoted(settings.database()));
		} catch (SQLException e) {
			throw new AssertionError("Unexpected error cleaning up database", e);
		}
	}

	private static String quoted(String raw) {
		return '"' + raw.replace("\"", "\"\"") + '"';
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(PostgresDriverConformanceTest.class);
}
