package works.bosk.drivers.postgresql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.type.TypeFactory;
import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import works.bosk.BoskDriver;
import works.bosk.BoskInfo;
import works.bosk.DriverFactory;
import works.bosk.Identifier;
import works.bosk.Reference;
import works.bosk.RootReference;
import works.bosk.StateTreeNode;
import works.bosk.exceptions.InvalidTypeException;
import works.bosk.exceptions.NotYetImplementedException;

import static com.fasterxml.jackson.databind.type.TypeFactory.rawClass;
import static java.util.Objects.requireNonNull;
import static works.bosk.util.Classes.mapValue;

public class PostgresDriver<R extends StateTreeNode> implements BoskDriver<R> {
	final BoskDriver<R> downstream;
	final RootReference<R> rootRef;
	final PostgresDriverSettings settings;
	final Connection connection;
	final ObjectMapper mapper;

	public PostgresDriver(
		BoskDriver<R> downstream,
		RootReference<R> rootRef,
		ObjectMapper mapper,
		PostgresDriverSettings settings
	) {
		this.downstream = requireNonNull(downstream);
		this.rootRef = requireNonNull(rootRef);
		this.mapper = requireNonNull(mapper);
		this.settings = requireNonNull(settings);
		this.connection = getConnection();
	}

	/**
	 * Note that all drivers from this factory will share a {@link Connection}.
	 */
	public static <RR extends StateTreeNode> PostgresDriverFactory<RR> factory(
		PostgresDriverSettings settings,
		Function<BoskInfo<RR>, ObjectMapper> objectMapperFactory
	) {
		// TODO: Validate host and database names
		return (b, d) -> new PostgresDriver<>(
			d,
			b.rootReference(),
			objectMapperFactory.apply(b),
			settings
		);
	}

	public void close() {
		try {
			connection.close();
		} catch (SQLException e) {
			LOGGER.warn("Unable to close connection", e);
			// This is a best-effort thing. Just continue.
		}
	}

	public interface PostgresDriverFactory<RR extends StateTreeNode> extends DriverFactory<RR> {
		@Override
		PostgresDriver<RR> build(BoskInfo<RR> boskInfo, BoskDriver<RR> downstream);
	}

	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		// TODO: Consider a disconnected mode where we delegate downstream if something goes wrong
		String json;
		try (
			var stmt = connection.createStatement()
		) {
			stmt.execute("""
				CREATE TABLE IF NOT EXISTS bosk_table (
					id char(7) PRIMARY KEY NOT NULL,
					state jsonb NOT NULL,
					diagnostics jsonb NOT NULL
				);
			""");
			stmt.execute("""
				BEGIN TRANSACTION;
			""");
			try (var resultSet = stmt.executeQuery("SELECT state FROM bosk_table WHERE id='current'")) {
				if (resultSet.next()) {
					json = resultSet.getString("state");
					stmt.execute("COMMIT TRANSACTION");
					JavaType valueType = TypeFactory.defaultInstance().constructType(rootType);
					return mapper.readValue(json, valueType);
				} else {
					R root = downstream.initialRoot(rootType);
					var insertStmt = connection.prepareStatement("""
							INSERT INTO bosk_table (id, state, diagnostics) VALUES ('current', ?::jsonb, ?::jsonb)
							ON CONFLICT DO NOTHING;

							COMMIT TRANSACTION
						""");
					insertStmt.setString(1, mapper.writerFor(rawClass(rootType)).writeValueAsString(
						root));
					insertStmt.setString(2, mapper.writerFor(mapValue(String.class)).writeValueAsString(
						rootRef.diagnosticContext().getAttributes()));
					insertStmt.executeUpdate();
					return root;
				}
			}
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	private Connection getConnection() {
		try {
			return DriverManager.getConnection(settings.url(), new Properties());
		} catch (SQLException e) {
			throw new NotYetImplementedException(e);
		}
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		try {
			executeCommand("""
					UPDATE bosk_table
					   SET state = ?::jsonb, diagnostics = ?::jsonb
					 WHERE id = 'current'
					""",
				writerFor(target.targetType())
					.writeValueAsString(newValue),
				writerFor(mapValue(String.class))
					.writeValueAsString(rootRef.diagnosticContext().getAttributes())
			);
		} catch (JsonProcessingException e) {
			throw new IllegalStateException(e);
		}
		// TODO: listen for updates
		downstream.submitReplacement(target, newValue);
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		// TODO: listen for updates
		downstream.submitConditionalReplacement(target, newValue, precondition, requiredValue);
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		// TODO: listen for updates
		downstream.submitInitialization(target, newValue);
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		// TODO: listen for updates
		downstream.submitDeletion(target);
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		// TODO: listen for updates
		downstream.submitConditionalDeletion(target, precondition, requiredValue);
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		// TODO: check for updates from Postgres
		downstream.flush();
	}

	String quotedIdentifier(String raw) {
		return '"' + raw.replace("\"", "\"\"") + '"';
	}

	private ObjectWriter writerFor(Type type) {
		return mapper.writerFor(TypeFactory.defaultInstance()
			.constructType(type));
	}

	private void executeCommand(String query, String... parameters) {
		try (
			var stmt = connection.prepareStatement(query)
		) {
			int parameterCount = 0;
			for (String parameter : parameters) {
				stmt.setString(++parameterCount, parameter);
			}
			stmt.execute();
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(PostgresDriver.class);
}
