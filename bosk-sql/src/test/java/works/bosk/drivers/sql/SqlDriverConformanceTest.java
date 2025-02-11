package works.bosk.drivers.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
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
import static works.bosk.jackson.JacksonPluginConfiguration.MapShape.ARRAY;
import static works.bosk.jackson.JacksonPluginConfiguration.MapShape.LINKED_MAP;

/**
 * Note that this doesn't actually exercise the driver much, other than to ensure
 * it correctly forwards updates downstream and doesn't throw any errors.
 */
@Testcontainers
class SqlDriverConformanceTest extends DriverConformanceTest {
	public static final String JDBC_URL = "jdbc:tc:postgresql:16:///?TC_DAEMON=true";
	private final Deque<Runnable> tearDownActions = new ArrayDeque<>();
	private static HikariDataSource dataSource;

	@BeforeAll
	static void setupConnectionPool() {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(JDBC_URL);
		dataSource = new HikariDataSource(config);
	}

	@BeforeEach
	void setupDriverFactory() {
		driverFactory = (boskInfo, downstream) -> {
			SqlDriverSettings settings = new SqlDriverSettings(
				200, 1, 1
			);
			tearDownActions.addFirst(this::cleanupTable);
			var driver = SqlDriver.<TestEntity>factory(
				settings, dataSource::getConnection,
				b -> new ObjectMapper()
					.enable(INDENT_OUTPUT)
					// TODO: SqlDriver should add this, not the caller! It's required for correctness
					.registerModule(new JacksonPlugin(new JacksonPluginConfiguration(ARRAY)).moduleFor(b))
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
		return dataSource.getConnection();
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(SqlDriverConformanceTest.class);
}
