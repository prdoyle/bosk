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
		MYSQL("mysql", "mysql", "bosk-test", "src/test/resources/mysql.dockerfile", "/var/lib/mysql"),
		POSTGRES("postgres", "postgresql", "bosk-test", "src/test/resources/postgresql.dockerfile", "/var/lib/postgresql"),
		SQLITE(dbName -> "jdbc:sqlite:" + TEMP_DIR.resolve(dbName + ".db")),
		;

		final Function<String, String> url;

		Database(String dockerName, String jdbcName, String tag, String dockerfilePath, String dataDir) {
			String imageTag = dockerName + ":" + tag;
			this.url = dbName -> {
				ensureImageIsBuilt(imageTag, dockerfilePath);
				return "jdbc:tc:" + jdbcName + ":" + tag
					+ ":///" + dbName
					+ "?TC_DAEMON=true"
					+ "&TC_TMPFS=" + dataDir + ":rw";
			};
		}

		Database(Function<String, String> url) {
			this.url = url;
		}

		private static final ConcurrentHashMap<String, Boolean> IMAGES_BUILT = new ConcurrentHashMap<>();

		private static void ensureImageIsBuilt(String imageTag, String dockerfilePath) {
			IMAGES_BUILT.computeIfAbsent(imageTag, _ ->
				new ImageFromDockerfile(imageTag)
					.withDockerfile(Paths.get(dockerfilePath)).get()
				!= null); // Super clever way to always generate `true`
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

	public static SqlDriverImpl.SqlDriverFactory<TestEntity> sqlDriverFactory(SqlDriverSettings settings, HikariDataSource dataSource) {
		return SqlDriver.factory(
			settings, dataSource::getConnection,
			(_, m) -> m
				.enable(INDENT_OUTPUT)
				.enable(INCLUDE_SOURCE_IN_LOCATION)
		);
	}

}
