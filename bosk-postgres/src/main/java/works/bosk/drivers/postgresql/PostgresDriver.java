package works.bosk.drivers.postgresql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.type.TypeFactory;
import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import works.bosk.BoskDriver;
import works.bosk.BoskInfo;
import works.bosk.DriverFactory;
import works.bosk.Identifier;
import works.bosk.Reference;
import works.bosk.RootReference;
import works.bosk.StateTreeNode;
import works.bosk.exceptions.FlushFailureException;
import works.bosk.exceptions.InvalidTypeException;

import static com.fasterxml.jackson.databind.type.TypeFactory.rawClass;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static works.bosk.util.Classes.mapValue;

public class PostgresDriver implements BoskDriver {
	final BoskDriver downstream;
	final RootReference<?> rootRef;
	final Connection connection;
	final Connection listenerConnection;
	final ObjectMapper mapper;

	final AtomicBoolean isOpen = new AtomicBoolean(true);

	final Statements S;

	/**
	 * This is a way of mimicking a proper change-listening setup until we get LISTEN/NOTIFY working.
	 */
	final ExecutorService background = Executors.newFixedThreadPool(1);

	/**
	 * The thread that does the Postgres LISTEN
	 */
	final ScheduledExecutorService listener = Executors.newScheduledThreadPool(1);

	PostgresDriver(
		ConnectionSource cs,
		RootReference<?> rootRef,
		ObjectMapper mapper,
		BoskDriver downstream
	) {
		this.downstream = requireNonNull(downstream);
		this.rootRef = requireNonNull(rootRef);
		this.mapper = requireNonNull(mapper);
		try {
			this.connection = cs.get();
			this.listenerConnection = cs.get();
			try (
				var stmt = listenerConnection.createStatement()
			) {
				stmt.execute("LISTEN bosk_changed");
			}
			this.S = new Statements(mapper);
		} catch (SQLException e) {
			throw new IllegalStateException("Unable to access PGConnection", e);
		}
		this.listener.scheduleWithFixedDelay(this::listenerLoop, 0, 5, SECONDS);
	}

	public interface ConnectionSource {
		Connection get() throws SQLException;
	}

	private void listenerLoop() {
		try {
			while (isOpen.get()) {
				PGNotification[] notifications = null;
				try {
					notifications = listenerConnection.unwrap(PGConnection.class).getNotifications();
				} catch (PSQLException e) {
					if (isOpen.get()) {
						throw e;
					} else {
						continue;
					}
				}
				if (notifications != null) {
					for (PGNotification n : notifications) {
						processNotification(Notification.from(n));
					}
				}
			}
		} catch (SQLException e) {
			LOGGER.error("Listener loop aborted; will wait and restart", e);
		}
	}

	private void processNotification(Notification n) {
		System.err.println("Hey hey! Notification: " + n);
	}

	/**
	 * Note that all drivers from this factory will share a {@link Connection}.
	 */
	public static <RR extends StateTreeNode> PostgresDriverFactory<RR> factory(
		ConnectionSource connectionSource,
		Function<BoskInfo<RR>, ObjectMapper> objectMapperFactory
	) {
		return (b, d) -> new PostgresDriver(
			connectionSource, b.rootReference(), objectMapperFactory.apply(b), d
		);
	}

	/**
	 * Best-effort cleanup, mainly meant for testing.
	 * The driver might still perform a small number of asynchronous actions after this method returns.
	 */
	public void close() {
		if (isOpen.getAndSet(false)) {
			listener.shutdown();
			try {
				connection.close();
				listenerConnection.close();
			} catch (SQLException e) {
				LOGGER.warn("Unable to close connection", e);
				// This is a best-effort thing. Just continue.
			}
		}
	}

	public interface PostgresDriverFactory<RR extends StateTreeNode> extends DriverFactory<RR> {
		@Override PostgresDriver build(BoskInfo<RR> boskInfo, BoskDriver downstream);
	}

	@Override
	public StateTreeNode initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		// TODO: Consider a disconnected mode where we delegate downstream if something goes wrong
		String json;
		try {
			S.executeCommand(connection, """
				CREATE TABLE IF NOT EXISTS bosk_changes (
					id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY NOT NULL,
					ref varchar NOT NULL,
					new_state jsonb NULL,
					diagnostics jsonb NOT NULL
				);
				""");

			S.executeCommand(connection, """
				CREATE TABLE IF NOT EXISTS bosk_table (
					id char(7) PRIMARY KEY NOT NULL,
					state jsonb NOT NULL
				);
				""");

			S.executeCommand(connection, """
				CREATE OR REPLACE FUNCTION notify_bosk_changed()
					RETURNS trigger AS $$
					BEGIN
						PERFORM pg_notify('bosk_changed', NEW.id::text);
						RETURN NEW;
					END;
					$$ LANGUAGE plpgsql;
				""");
			S.executeCommand(connection, """
				CREATE OR REPLACE TRIGGER bosk_changed
					AFTER UPDATE ON bosk_table
					  FOR EACH ROW EXECUTE FUNCTION notify_bosk_changed();
				""");
			S.beginTransaction(connection);
			try (
				var query = connection.prepareStatement("SELECT state FROM bosk_table WHERE id='current'");
				var resultSet = S.executeQuery(connection, query)
			) {
				if (resultSet.next()) {
					json = resultSet.getString("state");
					S.commitTransaction(connection);
					JavaType valueType = TypeFactory.defaultInstance().constructType(rootType);
					return mapper.readValue(json, valueType);
				} else {
					StateTreeNode root = downstream.initialRoot(rootType);
					String stateJson = mapper.writerFor(rawClass(rootType)).writeValueAsString(root);

					S.initializeState(connection, stateJson);
					S.insertChange(connection, rootRef, stateJson);
					S.commitTransaction(connection);

					return root;
				}
			}
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		try {
			String fieldPath = target.path().segmentStream()
				.map(PostgresDriver::stringLiteral)
				.collect(joining(",", "{", "}"));
			String newValueJson = writerFor(target.targetType())
				.writeValueAsString(newValue);
			S.beginTransaction(connection);
			executeCommand("""
					UPDATE bosk_table
					   SET
						state = jsonb_set(state, '%s', ?::jsonb, false)
					 WHERE id = 'current'
					""".formatted(fieldPath),
				newValueJson
			);
			S.insertChange(connection, rootRef, newValueJson);
			S.commitTransaction(connection);
		} catch (JsonProcessingException | SQLException e) {
			throw new IllegalStateException(e);
		}
		runInBackground(() -> downstream.submitReplacement(target, newValue));
	}

	private static String stringLiteral(String raw) {
		return '"' + raw.replace("\"", "\"\"") + '"';
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		runInBackground(() -> downstream.submitConditionalReplacement(target, newValue, precondition, requiredValue));
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		runInBackground(() -> downstream.submitInitialization(target, newValue));
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		runInBackground(() -> downstream.submitDeletion(target));
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		runInBackground(() -> downstream.submitConditionalDeletion(target, precondition, requiredValue));
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		try {
			// Wait for any background tasks to finish
			background.submit(()->{}).get();
		} catch (ExecutionException e) {
			throw new FlushFailureException(e);
		}
		downstream.flush();
	}

	private void runInBackground(Runnable runnable) {
		var attributes = rootRef.diagnosticContext().getAttributes();
		background.submit(() -> {
			try (var __ = rootRef.diagnosticContext().withOnly(attributes)) {
				runnable.run();
			}
		});
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

	private record Notification(
		String name,
		String parameter
	) {
		static Notification from(PGNotification pg) {
			return new Notification(pg.getName(), pg.getParameter());
		}
	}

	record Statements(
		ObjectMapper mapper
	) {
		void executeCommand(Connection connection, String query, String... parameters) throws SQLException {
			try (
				var stmt = connection.prepareStatement(query)
			) {
				int parameterCount = 0;
				for (String parameter : parameters) {
					stmt.setString(++parameterCount, parameter);
				}
				stmt.execute();
			}
		}

		ResultSet executeQuery(Connection connection, PreparedStatement query, String... parameters) throws SQLException {
			int parameterCount = 0;
			for (String parameter : parameters) {
				query.setString(++parameterCount, parameter);
			}
			return query.executeQuery();
		}

		void beginTransaction(Connection c) throws SQLException {
			executeCommand(c, "BEGIN TRANSACTION");
		}

		void commitTransaction(Connection c) throws SQLException {
			executeCommand(c, "COMMIT TRANSACTION");
		}

		void initializeState(Connection c, String newValue) throws SQLException {
			executeCommand(c, """
				INSERT INTO bosk_table (id, state) VALUES ('current', ?::jsonb)
				ON CONFLICT DO NOTHING;
				""", newValue);
		}

		void insertChange(Connection c, Reference<?> ref, String newValue) throws JsonProcessingException, SQLException {
			executeCommand(c, """
				INSERT INTO bosk_changes (ref, new_state, diagnostics) VALUES (?, ?::jsonb, ?::jsonb)
				RETURNING id;
				""",
				ref.pathString(),
				newValue,
				mapper.writerFor(mapValue(String.class)).writeValueAsString(
					ref.root().diagnosticContext().getAttributes())
			);
		}
	}

}
