package works.bosk.drivers.sql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.testcontainers.images.builder.ImageFromDockerfile;
import works.bosk.exceptions.NotYetImplementedException;
import works.bosk.testing.drivers.state.TestEntity;

import static tools.jackson.core.StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION;
import static tools.jackson.databind.SerializationFeature.INDENT_OUTPUT;

public class SqlTestService {
	static {
		// Build images from Dockerfiles so Dependabot can track updates
		new ImageFromDockerfile("mysql:bosk-test")
			.withDockerfile(Paths.get("src/test/resources/mysql.dockerfile")).get();
		new ImageFromDockerfile("postgres:bosk-test")
			.withDockerfile(Paths.get("src/test/resources/postgresql.dockerfile")).get();
	}

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
		MYSQL(testcontainers("mysql:bosk-test", "/var/lib/mysql")),
		POSTGRES(testcontainers("postgresql:bosk-test", "/var/lib/postgresql")),
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

	private static Function<String, String> testcontainers(String image, String dataDir) {
		return dbName -> "jdbc:tc:" + image
			+ ":///" + dbName
			+ "?TC_DAEMON=true"
			+ "&TC_TMPFS=" + dataDir + ":rw"
			;
	}

	public static SqlDriverImpl.SqlDriverFactory<TestEntity> sqlDriverFactory(SqlDriverSettings settings, HikariDataSource dataSource) {
		return SqlDriver.factory(
			settings, dataSource::getConnection,
			(_, m) -> m
				.enable(INDENT_OUTPUT)
				.enable(INCLUDE_SOURCE_IN_LOCATION)
		);
	}

}
