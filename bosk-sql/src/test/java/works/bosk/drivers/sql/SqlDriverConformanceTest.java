package works.bosk.drivers.sql;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;
import works.bosk.drivers.SharedDriverConformanceTest;
import works.bosk.drivers.sql.SqlTestService.Database;
import works.bosk.drivers.sql.schema.Schema;
import works.bosk.junit.ParametersByName;

import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.using;
import static works.bosk.drivers.sql.SqlTestService.Database.POSTGRES;
import static works.bosk.drivers.sql.SqlTestService.dataSourceFor;
import static works.bosk.drivers.sql.SqlTestService.sqlDriverFactory;

@Testcontainers
class SqlDriverConformanceTest extends SharedDriverConformanceTest {
	private final Deque<Runnable> tearDownActions = new ArrayDeque<>();
	private final HikariDataSource dataSource;
	private SqlDriverSettings settings;

	@ParametersByName
	SqlDriverConformanceTest(Database database) {
		this.dataSource = dataSourceFor(database);
	}

	@SuppressWarnings("unused")
	public static Stream<Database> database() {
		return Stream.of(POSTGRES); // MYSQL is very slow for some reason
	}

	@BeforeEach
	void setupDriverFactory() {
		settings = new SqlDriverSettings(
			getClass().getSimpleName(), 50, 100
		);
		driverFactory = (boskInfo, downstream) -> {
			var driver = sqlDriverFactory(settings, dataSource).build(boskInfo, downstream);
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
		Schema schema = new works.bosk.drivers.sql.schema.Schema(name(settings.schemaName()));
		try (
			var c = dataSource.getConnection()
		) {
			using(c)
				.dropSchemaIfExists(schema)
				.cascade()
				.execute();
			LOGGER.trace("Dropped schema {}", schema);
		} catch (SQLException e) {
			throw new AssertionError("Unexpected error cleaning up table", e);
		}
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(SqlDriverConformanceTest.class);
}
