package works.bosk.drivers.sql;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.junit.jupiter.Testcontainers;
import works.bosk.drivers.SharedDriverConformanceTest;
import works.bosk.drivers.sql.SqlTestService.Database;
import works.bosk.drivers.sql.schema.Schema;
import works.bosk.junit.ParametersByName;

import static works.bosk.drivers.sql.SqlTestService.Database.POSTGRES;
import static works.bosk.drivers.sql.SqlTestService.Database.SQLITE;
import static works.bosk.drivers.sql.SqlTestService.sqlDriverFactory;

@Testcontainers
class SqlDriverConformanceTest extends SharedDriverConformanceTest {
	private final Deque<Runnable> tearDownActions = new ArrayDeque<>();
	private final Database database;
	private final AtomicInteger dbCounter = new AtomicInteger(0);
	private SqlDriverSettings settings;
	private HikariDataSource dataSource;

	@ParametersByName
	SqlDriverConformanceTest(Database database) {
		this.database = database;
	}

	@SuppressWarnings("unused")
	public static Stream<Database> database() {
		return Stream.of(POSTGRES, SQLITE); // MYSQL is very slow for some reason
	}

	@BeforeEach
	void setupDriverFactory() {
		settings = new SqlDriverSettings(
			50, 100
		);
		String databaseName = SqlDriverConformanceTest.class.getSimpleName()
			+ dbCounter.incrementAndGet();
		dataSource = database.dataSourceFor(databaseName);
		driverFactory = (boskInfo, downstream) -> {
			var driver = sqlDriverFactory(settings, dataSource).build(boskInfo, downstream);
			tearDownActions.addFirst(driver::close);
			return driver;
		};
	}

	@AfterEach
	void runTearDown() throws SQLException {
		tearDownActions.forEach(Runnable::run);
		cleanupTables();
	}

	private void cleanupTables() throws SQLException {
		try (
			var c = dataSource.getConnection()
		) {
			new Schema().dropTables(c);
		}
	}

}
