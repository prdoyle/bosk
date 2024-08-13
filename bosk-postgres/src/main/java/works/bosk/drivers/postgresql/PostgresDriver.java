package works.bosk.drivers.postgresql;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import works.bosk.BoskDriver;
import works.bosk.BoskInfo;
import works.bosk.DriverFactory;
import works.bosk.Identifier;
import works.bosk.Reference;
import works.bosk.StateTreeNode;
import works.bosk.exceptions.InvalidTypeException;
import works.bosk.exceptions.NotYetImplementedException;

import static com.fasterxml.jackson.databind.type.TypeFactory.rawClass;

@RequiredArgsConstructor
public class PostgresDriver<R extends StateTreeNode> implements BoskDriver<R> {
	final BoskDriver<R> downstream;
	final ObjectMapper mapper;
	final PostgresDriverSettings settings;

	/**
	 * Note that all drivers from this factory will share a {@link Connection}.
	 */
	public static <RR extends StateTreeNode> DriverFactory<RR> factory(
		PostgresDriverSettings settings,
		Function<BoskInfo<RR>, ObjectMapper> objectMapperFactory
	) {
		// TODO: Validate host and database names
		return (b, d) -> new PostgresDriver<>(
			d,
			objectMapperFactory.apply(b),
			settings
		);
	}

	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		// TODO: Consider a disconnected mode where we delegate downstream if something goes wrong
		String json;
		try (
			var connection = getConnection();
			var stmt = connection.createStatement()
		) {
			String schema = quotedIdentifier(settings.schema());
			String query = """
					CREATE SCHEMA IF NOT EXISTS %s;

					CREATE TABLE IF NOT EXISTS bosk_table (
				 				id varchar(10) PRIMARY KEY NOT NULL,
				 				state jsonb NOT NULL
					);

					BEGIN TRANSACTION;
				""".formatted(schema);
			stmt.execute(query);
			try (var resultSet = stmt.executeQuery("SELECT state FROM bosk_table WHERE id='current'")) {
				if (resultSet.next()) {
					stmt.execute("COMMIT TRANSACTION");
					json = resultSet.getString("state");
					JavaType valueType = TypeFactory.defaultInstance().constructType(rootType);
					return mapper.readValue(json, valueType);
				} else {
					R root = downstream.initialRoot(rootType);
					var insertStmt = connection.prepareStatement("""
						INSERT INTO bosk_table (id, state) VALUES ('current', ?::jsonb)
						ON CONFLICT DO NOTHING
					""");
					insertStmt.setString(1, mapper.writerFor(rawClass(rootType)).writeValueAsString(root));
					insertStmt.executeUpdate();
					return root;
				}
			}
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	private Connection getConnection() throws SQLException {
		return DriverManager.getConnection(settings.url(), new Properties());
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		throw new NotYetImplementedException();
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		throw new NotYetImplementedException();
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		throw new NotYetImplementedException();
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		throw new NotYetImplementedException();
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		throw new NotYetImplementedException();
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		downstream.flush();
		throw new NotYetImplementedException();
	}

	String quotedIdentifier(String raw) {
		return '"' + raw.replace("\"", "\"\"") + '"';
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(PostgresDriver.class);
}
