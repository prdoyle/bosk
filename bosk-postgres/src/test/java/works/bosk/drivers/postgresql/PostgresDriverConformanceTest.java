package works.bosk.drivers.postgresql;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;
import works.bosk.drivers.DriverConformanceTest;
import works.bosk.drivers.state.TestEntity;
import works.bosk.jackson.JacksonPlugin;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;

@Testcontainers
class PostgresDriverConformanceTest extends DriverConformanceTest {
//	static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");
	public static final String JDBC_URL = "jdbc:tc:postgresql:16:///?TC_DAEMON=true";
	private final Deque<Runnable> tearDownActions = new ArrayDeque<>();

	@BeforeAll
	static void setupDatabase() {
//		POSTGRES.start();
//		try (
//			var connection = getInitialConnection();
//			var stmt = connection.createStatement()
//		) {
//			stmt.execute("CREATE DATABASE " + quoted(DATABASE));
//		} catch (SQLException e) {
//			throw new AssertionError("Unexpected error setting up database", e);
//		}
	}

	@AfterAll
	static void shutdown() {
//		POSTGRES.stop();;
	}

	@BeforeEach
	void setupDriverFactory() {
		driverFactory = (boskInfo, downstream) -> {
			PostgresDriverSettings settings = new PostgresDriverSettings(
				JDBC_URL
			);
			tearDownActions.addFirst(this::cleanupTable);
			var driver = PostgresDriver.<TestEntity>factory(
				settings,
				b -> new ObjectMapper()
					.enable(INDENT_OUTPUT)
					.registerModule(new JacksonPlugin().moduleFor(b))
			).build(boskInfo, downstream);
			tearDownActions.addLast(driver::close);
			return driver;
		};
	}

	@AfterEach
	void runTearDown() {
		tearDownActions.forEach(Runnable::run);
	}

	private void cleanupTable() {
		try (
			var connection = getDBConnection();
			var stmt = connection.createStatement()
		) {
			stmt.execute("DROP TABLE IF EXISTS bosk_table");
		} catch (SQLException e) {
			throw new AssertionError("Unexpected error cleaning up table", e);
		}
	}

	private static Connection getDBConnection() throws SQLException {
		return DriverManager.getConnection(JDBC_URL, new Properties());
	}

	private static String quoted(String raw) {
		return '"' + raw.replace("\"", "\"\"") + '"';
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(PostgresDriverConformanceTest.class);
}
