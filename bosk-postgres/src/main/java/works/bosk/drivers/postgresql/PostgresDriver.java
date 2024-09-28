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
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
import works.bosk.Listing;
import works.bosk.MapValue;
import works.bosk.Path;
import works.bosk.Reference;
import works.bosk.RootReference;
import works.bosk.SideTable;
import works.bosk.StateTreeNode;
import works.bosk.exceptions.FlushFailureException;
import works.bosk.exceptions.InvalidTypeException;
import works.bosk.exceptions.NotYetImplementedException;

import static com.fasterxml.jackson.databind.type.TypeFactory.rawClass;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class PostgresDriver implements BoskDriver {
	final BoskDriver downstream;
	final RootReference<?> rootRef;
	final ConnectionSource connectionSource;
	final ObjectMapper mapper;

	/**
	 * Long-lived connection for LISTEN / NOTIFY
	 */
	final Connection listenerConnection;

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

	private final AtomicLong lastChangeSubmittedDownstream = new AtomicLong(-1);

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
			connectionSource = cs;
			this.listenerConnection = connectionSource.get();
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
		} catch (Throwable e) {
			LOGGER.error("Listener loop aborted; will wait and restart", e);
		}
	}

	private void processNotification(Notification n) throws SQLException {
		try (
			var c = connectionSource.get();
			var q = c.prepareStatement("SELECT ref, new_state, diagnostics, id FROM bosk_changes WHERE id = ?::int");
			var rs = S.executeQuery(c, q, n.parameter)
		) {
			if (rs.next()) {
				var ref = rs.getString(1);
				var newState = rs.getString(2);
				var diagnostics = rs.getString(3);
				record Change(String ref, String newState, String diagnostics){}
				LOGGER.debug("Received change notification: {}", new Change(ref, newState, diagnostics));

				MapValue<String> diagnosticAttributes;
				if (diagnostics == null) {
					diagnosticAttributes = MapValue.empty();
				} else {
					try {
						diagnosticAttributes = mapper.readerFor(mapValueType(String.class)).readValue(diagnostics);
					} catch (JsonProcessingException e) {
						LOGGER.error("Unable to parse diagnostic attributes; ignoring", e);
						diagnosticAttributes = MapValue.empty();
					}
				}
				try (var __ = rootRef.diagnosticContext().withOnly(diagnosticAttributes)) {
					Reference<Object> target = rootRef.then(Object.class, Path.parse(ref));
					if (newState == null) {
						LOGGER.debug("Downstream submitDeletion({})", target);
						downstream.submitDeletion(target);
					} else {
						Object newValue = mapper.readerFor(TypeFactory.defaultInstance().constructType(target.targetType()))
								.readValue(newState);
						LOGGER.debug("Downstream submitReplacement({}, ...)", target);
						downstream.submitReplacement(target, newValue);
					}
					long changeID = rs.getLong(4);
					long prev = lastChangeSubmittedDownstream.getAndSet(changeID);
					if (prev >= changeID) {
						LOGGER.error("Change ID did not increase; expected {} > {}", changeID, prev);
					}
				} catch (JsonProcessingException e) {
					throw new NotYetImplementedException("Error parsing notification", e);
				} catch (InvalidTypeException e) {
					throw new NotYetImplementedException("Invalid object reference: \"" + ref + "\"", e);
				}
			} else {
				LOGGER.error("Received notification of nonexistent change; bosk state may be unreliable: {}", n);
				throw new NotYetImplementedException("Should reconnect and reload the state from scratch");
			}
		}
	}

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
		try (
			var connection = connectionSource.get()
		){
			S.beginTransaction(connection);
			ensureTablesExist(connection);
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

	private void ensureTablesExist(Connection connection) throws SQLException {
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
				AFTER INSERT ON bosk_changes
				  FOR EACH ROW EXECUTE FUNCTION notify_bosk_changed();
			""");
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		try (
			var connection = connectionSource.get()
		){
			String newValueJson = writerFor(target.targetType())
				.writeValueAsString(newValue);
			S.beginTransaction(connection);
			if (S.enclosingObjectExists(connection, target)) {
				S.setField(connection, target, newValueJson);
				S.insertChange(connection, target, newValueJson);
				S.commitTransaction(connection);
			} else {
				S.rollbackTransaction(connection);
			}
		} catch (JsonProcessingException | SQLException e) {
			throw new IllegalStateException(e);
		}
//		runInBackground(() -> downstream.submitReplacement(target, newValue));
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		try (
			var connection = connectionSource.get()
		){
			String newValueJson = writerFor(target.targetType())
				.writeValueAsString(newValue);
			S.beginTransaction(connection);
			Identifier preconditionValue = S.readState(connection, precondition);
			if (requiredValue.equals(preconditionValue) && S.enclosingObjectExists(connection, target)) {
				S.setField(connection, target, newValueJson);
				S.insertChange(connection, target, newValueJson);
				S.commitTransaction(connection);
			} else {
				S.rollbackTransaction(connection);
			}
		} catch (JsonProcessingException | SQLException e) {
			throw new IllegalStateException(e);
		}
//		runInBackground(() -> downstream.submitConditionalReplacement(target, newValue, precondition, requiredValue));
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		try (
			var connection = connectionSource.get()
		){
			String newValueJson = writerFor(target.targetType())
				.writeValueAsString(newValue);
			S.beginTransaction(connection);
			if (S.referenceExists(connection, target) && S.enclosingObjectExists(connection, target)) {
				S.rollbackTransaction(connection);
			} else {
				S.setField(connection, target, newValueJson);
				S.insertChange(connection, target, newValueJson);
				S.commitTransaction(connection);
			}
		} catch (JsonProcessingException | SQLException e) {
			throw new IllegalStateException(e);
		}
//		runInBackground(() -> downstream.submitConditionalReplacement(target, newValue, precondition, requiredValue));
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		try (
			var connection = connectionSource.get()
		){
			S.beginTransaction(connection);
			if (S.enclosingObjectExists(connection, target)) {
				S.deleteField(connection, target);
				S.insertChange(connection, target, null);
				S.commitTransaction(connection);
			} else {
				S.rollbackTransaction(connection);
			}
		} catch (JsonProcessingException | SQLException e) {
			throw new IllegalStateException(e);
		}
//		runInBackground(() -> downstream.submitDeletion(target));
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		try (
			var connection = connectionSource.get()
		){
			S.beginTransaction(connection);
			Identifier preconditionValue = S.readState(connection, precondition);
			if (requiredValue.equals(preconditionValue) && S.enclosingObjectExists(connection, target)) {
				S.deleteField(connection, target);
				S.insertChange(connection, target, null);
				S.commitTransaction(connection);
			} else {
				S.rollbackTransaction(connection);
			}
		} catch (JsonProcessingException | SQLException e) {
			throw new IllegalStateException(e);
		}
//		runInBackground(() -> downstream.submitDeletion(target));
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		try {
			long currentChangeID = S.latestChangeID(connectionSource.get());
			// Wait for any background tasks to finish
			background.submit(()->{}).get();
			// Wait for any pending notifications
			while (lastChangeSubmittedDownstream.get() < currentChangeID) {
				// TODO: Quit after a while
				Thread.sleep(100);
			}
		} catch (ExecutionException  | SQLException e) {
			throw new FlushFailureException(e);
		}
		downstream.flush();
	}

	private static String stringLiteral(String raw) {
		return '"' + raw.replace("\"", "\"\"") + '"';
	}

	/**
	 * Fake it till you make it
	 */
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

	private static final Logger LOGGER = LoggerFactory.getLogger(PostgresDriver.class);

	private record Notification(
		String name,
		String parameter
	) {
		static Notification from(PGNotification pg) {
			return new Notification(pg.getName(), pg.getParameter());
		}
	}

	/**
	 * Low-level SQL queries.
	 * <p>
	 * These should be based on an individual change to a table, as opposed to some higher level abstract objective.
	 */
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

		void rollbackTransaction(Connection c) throws SQLException {
			executeCommand(c, "ROLLBACK TRANSACTION");
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
				mapper.writerFor(mapValueType(String.class)).writeValueAsString(
					ref.root().diagnosticContext().getAttributes())
			);
		}

		void setField(Connection connection, Reference<?> ref, String newValueJson) throws SQLException {
			executeCommand(connection, """
					UPDATE bosk_table
					   SET
						state = jsonb_set(state, '%s', ?::jsonb)
					 WHERE id = 'current'
					""".formatted(fieldPath(ref)),
				requireNonNull(newValueJson)
			);
		}

		void deleteField(Connection connection, Reference<?> ref) throws SQLException {
			executeCommand(connection, """
					UPDATE bosk_table
					   SET
						state = state #- '%s'
					 WHERE id = 'current'
					""".formatted(fieldPath(ref))
			);
		}

		/**
		 * @return null if nonexistent
		 */
		Identifier readState(Connection connection, Reference<Identifier> ref) throws SQLException {
			try (
				var q = connection.prepareStatement(
					"""
					SELECT state #>> '%s'
					  FROM bosk_table
					 WHERE id = 'current'
					""".formatted(fieldPath(ref)));
				var r = executeQuery(connection, q)
			) {
				if (r.next()) {
					String id = r.getString(1);
					if (id == null) {
						return null;
					} else {
						return Identifier.from(id);
					}
				} else {
					throw new NotYetImplementedException("Row disappeared");
				}
			}
		}

		boolean enclosingObjectExists(Connection connection, Reference<?> ref) throws SQLException {
			if (ref.path().isEmpty()) {
				return true;
			}
			try (
				var q = connection.prepareStatement(
					"""
					SELECT (state #>> '%s') IS NOT NULL
					  FROM bosk_table
					 WHERE id = 'current'
					""".formatted(fieldPath(ref.enclosingReference(Object.class))));
				var r = executeQuery(connection, q)
			) {
				if (r.next()) {
					return r.getBoolean(1);
				} else {
					throw new NotYetImplementedException("Row disappeared");
				}
			} catch (InvalidTypeException e) {
				throw new AssertionError("Every non-root path must have a valid enclosing reference", e);
			}
		}

		boolean referenceExists(Connection connection, Reference<?> ref) throws SQLException {
			try (
				var q = connection.prepareStatement(
					"""
					SELECT (state #>> '%s') IS NOT NULL
					  FROM bosk_table
					 WHERE id = 'current'
					""".formatted(fieldPath(ref)));
				var r = executeQuery(connection, q)
			) {
				if (r.next()) {
					boolean result = r.getBoolean(1);
					return result;
				} else {
					throw new NotYetImplementedException("Row disappeared");
				}
			}
		}

		long latestChangeID(Connection connection) throws SQLException {
			try (
				var q = connection.prepareStatement(
					"SELECT max(id) FROM bosk_changes"
				);
				var r = executeQuery(connection, q)
			) {
				r.next();
				return r.getLong(1);
			}
		}
	}

	private static String fieldPath(Reference<?> ref) {
		ArrayList<String> steps = new ArrayList<>();
		buildFieldPath(ref, steps);
		return "{" + String.join(",", steps) + "}";
	}

	private static void buildFieldPath(Reference<?> ref, ArrayList<String> steps) {
		if (!ref.path().isEmpty()) {
			Reference<Object> enclosing = enclosingReference(ref);
			buildFieldPath(enclosing, steps);
			if (Listing.class.isAssignableFrom(enclosing.targetClass())) {
				steps.add(stringLiteral("ids"));
			} else if (SideTable.class.isAssignableFrom(enclosing.targetClass())) {
				steps.add(stringLiteral("valuesById"));
			}
			steps.add(stringLiteral(ref.path().lastSegment()));
		}
	}

	private static Reference<Object> enclosingReference(Reference<?> ref) {
		assert !ref.path().isEmpty();
		try {
			return ref.enclosingReference(Object.class);
		} catch (InvalidTypeException e) {
			throw new AssertionError("Non-empty path should always have an enclosing path", e);
		}
	}

	private static JavaType mapValueType(Class<?> entryType) {
		return TypeFactory.defaultInstance().constructParametricType(MapValue.class, entryType);
	}
}
