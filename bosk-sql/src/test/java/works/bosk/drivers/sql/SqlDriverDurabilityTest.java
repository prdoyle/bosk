package works.bosk.drivers.sql;

import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;
import works.bosk.Bosk;
import works.bosk.drivers.AbstractDriverTest;
import works.bosk.drivers.sql.SqlTestService.Database;
import works.bosk.drivers.sql.schema.Schema;
import works.bosk.drivers.state.TestEntity;
import works.bosk.exceptions.InvalidTypeException;
import works.bosk.junit.ParametersByName;

import static org.jooq.impl.DSL.using;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static works.bosk.drivers.sql.SqlTestService.Database.POSTGRES;
import static works.bosk.drivers.sql.SqlTestService.sqlDriverFactory;

@Testcontainers
public class SqlDriverDurabilityTest extends AbstractDriverTest {
	private final Database database;
	SqlDriverSettings settings;
	HikariDataSource dataSource;
	AtomicInteger dbCounter = new AtomicInteger(0);

	@ParametersByName
	SqlDriverDurabilityTest(Database database) {
		this.database = database;
	}

	@SuppressWarnings("unused")
	static Stream<Database> database() {
		return Stream.of(POSTGRES);
	}

	@BeforeEach
	void initializeSettings() {
		settings = new SqlDriverSettings(1000, 100);
		dataSource = database.dataSourceFor(SqlDriverDurabilityTest.class.getSimpleName() + dbCounter.incrementAndGet());
	}

	@ParametersByName
	void tablesDropped_recovers() throws SQLException, IOException, InterruptedException {
		LOGGER.debug("Initialize database");
		var factory = sqlDriverFactory(settings, dataSource);
		var schema = new Schema();
		setupBosksAndReferences(factory);
		assertCorrectBoskContents();

		LOGGER.debug("Drop tables");
		try (var c = dataSource.getConnection()) {
			using(c)
				.dropTable(schema.BOSK)
				.execute();

			using(c)
				.dropTable(schema.CHANGES)
				.execute();
		}

		LOGGER.debug("Use another bosk to recreate the database");
		var fixer = new Bosk<>("fixer", TestEntity.class, SqlDriverDurabilityTest::differentInitialRoot, factory);

		LOGGER.debug("State should be restored");

		bosk.driver().flush();

		TestEntity actual;
		try (var __ = bosk.readContext()) {
			actual = bosk.rootReference().value();
		}

		TestEntity expected;
		try (var __ = fixer.readContext()) {
			expected = fixer.rootReference().value();
		}

		assertEquals(expected, actual);
	}

	private static @NotNull TestEntity differentInitialRoot(Bosk<TestEntity> b) throws InvalidTypeException {
		return AbstractDriverTest.initialRoot(b).withString("Different");
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(SqlDriverDurabilityTest.class);
}
