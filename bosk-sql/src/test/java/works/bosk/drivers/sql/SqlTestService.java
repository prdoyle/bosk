package works.bosk.drivers.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.concurrent.ConcurrentHashMap;
import works.bosk.drivers.state.TestEntity;
import works.bosk.jackson.JacksonPlugin;
import works.bosk.jackson.JacksonPluginConfiguration;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;
import static works.bosk.jackson.JacksonPluginConfiguration.MapShape.ARRAY;

public class SqlTestService {
	private static final ConcurrentHashMap<Database, HikariDataSource> DATA_SOURCES = new ConcurrentHashMap<>();

	public enum Database {
		POSTGRES("postgresql:16"),
		MYSQL("mysql:8.0.36");

		final String image;

		Database(String image) {
			this.image = image;
		}
	}

	public static HikariDataSource dataSourceFor(Database database) {
		return DATA_SOURCES.computeIfAbsent(database, SqlTestService::newHikariDataSource);
	}

	private static HikariDataSource newHikariDataSource(Database database) {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl("jdbc:tc:" + database.image + ":///?TC_DAEMON=true");
		return new HikariDataSource(config);
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
