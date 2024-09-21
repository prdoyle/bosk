package works.bosk.drivers.postgresql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Properties;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;
import works.bosk.drivers.DriverConformanceTest;
import works.bosk.drivers.state.TestEntity;
import works.bosk.jackson.JacksonPlugin;
import works.bosk.jackson.JacksonPluginConfiguration;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;
import static works.bosk.jackson.JacksonPluginConfiguration.MapShape.LINKED_MAP;

/**
 * Note that this doesn't actually exercise the driver much, other than to ensure
 * it correctly forwards updates downstream and doesn't throw any errors.
 */
@Testcontainers
class PostgresDriverConformanceTest extends DriverConformanceTest {
	public static final String JDBC_URL = "jdbc:tc:postgresql:16:///?TC_DAEMON=true";
	private static DataSource dataSource;
	private final Deque<Runnable> tearDownActions = new ArrayDeque<>();

	@BeforeAll
	static void setupConnectionPool() {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(JDBC_URL);
		dataSource = new HikariDataSource(config);
	}

	@BeforeEach
	void setupDriverFactory() {
		driverFactory = (boskInfo, downstream) -> {
			tearDownActions.addFirst(this::cleanupTable);
			var driver = PostgresDriver.<TestEntity>factory(
				dataSource::getConnection,
				new PostgresDriverSettings(),
				b -> new ObjectMapper()
					.enable(INDENT_OUTPUT)
					.registerModule(new JacksonPlugin(new JacksonPluginConfiguration(LINKED_MAP)).moduleFor(b))
			).build(boskInfo, downstream);
			tearDownActions.addFirst(driver::close);
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

	private static final Logger LOGGER = LoggerFactory.getLogger(PostgresDriverConformanceTest.class);
}
