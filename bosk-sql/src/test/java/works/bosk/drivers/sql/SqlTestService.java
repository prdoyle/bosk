package works.bosk.drivers.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import works.bosk.drivers.state.TestEntity;
import works.bosk.exceptions.NotYetImplementedException;
import works.bosk.jackson.JacksonPlugin;
import works.bosk.jackson.JacksonPluginConfiguration;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;
import static works.bosk.jackson.JacksonPluginConfiguration.MapShape.ARRAY;

public class SqlTestService {
	record DBKey(Database database, String databaseName) {}
	private static final ConcurrentHashMap<DBKey, HikariDataSource> DATA_SOURCES = new ConcurrentHashMap<>();

	static final Path TEMP_DIR;

	static {
		try {
			TEMP_DIR = Files.createTempDirectory("bosk-test");
			TEMP_DIR.toFile().deleteOnExit();
		} catch (IOException e) {
			throw new NotYetImplementedException(e);
		}
	}

	public enum Database {
		MYSQL(testcontainers("mysql:8.0.36")),
		POSTGRES(testcontainers("postgresql:16")),
		SQLITE(dbName -> "jdbc:sqlite:" + TEMP_DIR.resolve(dbName + ".db")),
		;

		final Function<String, String> url;

		Database(Function<String, String> url) {
			this.url = url;
		}

		public HikariDataSource dataSourceFor(String databaseName) {
			return DATA_SOURCES.computeIfAbsent(new DBKey(this, databaseName), SqlTestService::newHikariDataSource);
		}
	}

	private static HikariDataSource newHikariDataSource(DBKey key) {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(key.database().url.apply(key.databaseName()));
		config.setAutoCommit(false);
		return new HikariDataSource(config);
	}

	private static Function<String, String> testcontainers(String image) {
		return dbName -> "jdbc:tc:" + image + ":///" + dbName + "?TC_DAEMON=true";
	}

	public static SqlDriver.SqlDriverFactory<TestEntity> sqlDriverFactory(SqlDriverSettings settings, HikariDataSource dataSource) {
		return SqlDriver.factory(
			settings, dataSource::getConnection,
			b -> new ObjectMapper()
				.enable(INDENT_OUTPUT)
				// TODO: SqlDriver should add this, not the caller! It's required for correctness
				.registerModule(new JacksonPlugin(new JacksonPluginConfiguration(ARRAY)).moduleFor(b))
		);
	}

}
