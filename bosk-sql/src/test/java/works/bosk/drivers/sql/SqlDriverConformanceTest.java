package works.bosk.drivers.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;
import works.bosk.drivers.SharedDriverConformanceTest;
import works.bosk.drivers.state.TestEntity;
import works.bosk.jackson.JacksonPlugin;
import works.bosk.jackson.JacksonPluginConfiguration;
import works.bosk.junit.ParametersByName;
import works.bosk.junit.Slow;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;
import static org.jooq.impl.DSL.using;
import static works.bosk.drivers.sql.SqlDriverConformanceTest.Database.POSTGRES;
import static works.bosk.drivers.sql.schema.Schema.BOSK;
import static works.bosk.drivers.sql.schema.Schema.CHANGES;
import static works.bosk.jackson.JacksonPluginConfiguration.MapShape.ARRAY;

@Slow
@Testcontainers
class SqlDriverConformanceTest extends SharedDriverConformanceTest {
	private final Deque<Runnable> tearDownActions = new ArrayDeque<>();
	private final HikariDataSource dataSource;

	enum Database {
		POSTGRES("postgresql:16"),
		MYSQL("mysql:8.0.36");

		final String image;

		Database(String image) {
			this.image = image;
		}
	}

	private static final ConcurrentHashMap<Database, HikariDataSource> DATA_SOURCES = new ConcurrentHashMap<>();

	@ParametersByName
	SqlDriverConformanceTest(Database database) {
		this.dataSource = DATA_SOURCES.computeIfAbsent(database, SqlDriverConformanceTest::newHikariDataSource);
	}

	@SuppressWarnings("unused")
	public static Stream<Database> database() {
		return Stream.of(POSTGRES); // MYSQL is very slow for some reason
	}

	private static HikariDataSource newHikariDataSource(Database database) {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl("jdbc:tc:" + database.image + ":///?TC_DAEMON=true");
		return new HikariDataSource(config);
	}

	@BeforeEach
	void setupDriverFactory() {
		driverFactory = (boskInfo, downstream) -> {
			SqlDriverSettings settings = new SqlDriverSettings(
				50, 100
			);
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
		cleanupTables();
	}

	private void cleanupTables() {
		try (
			var c = dataSource.getConnection()
		) {
			using(c).dropTableIfExists(BOSK).execute();
			using(c).dropTableIfExists(CHANGES).execute();
			LOGGER.trace("Tables dropped: {}, {}", BOSK, CHANGES);
		} catch (SQLException e) {
			throw new AssertionError("Unexpected error cleaning up table", e);
		}
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(SqlDriverConformanceTest.class);
}
