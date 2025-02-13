package works.bosk.drivers.sql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import works.bosk.BoskDriver;
import works.bosk.BoskInfo;
import works.bosk.Catalog;
import works.bosk.CatalogReference;
import works.bosk.DriverFactory;
import works.bosk.Entity;
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
import works.bosk.jackson.JsonNodeSurgeon;
import works.bosk.jackson.JsonNodeSurgeon.NodeInfo;
import works.bosk.jackson.JsonNodeSurgeon.NodeLocation.NonexistentParent;
import works.bosk.jackson.JsonNodeSurgeon.NodeLocation.Root;

import static java.lang.Math.multiplyExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SqlDriver implements BoskDriver {
	final SqlDriverSettings settings;
	final BoskDriver downstream;
	final RootReference<?> rootRef;
	final ConnectionSource connectionSource;
	final ObjectMapper mapper;
	final JsonNodeSurgeon surgeon = new JsonNodeSurgeon();

	final AtomicBoolean isOpen = new AtomicBoolean(true);

	final Statements S;

	/**
	 * This is a way of mimicking a proper change-listening setup until we get polling working.
	 */
	final ExecutorService background = Executors.newFixedThreadPool(1);

	/**
	 * The thread that does the Postgres LISTEN
	 */
	final ScheduledExecutorService listener;

	private final AtomicLong lastChangeSubmittedDownstream = new AtomicLong(-1);

	SqlDriver(
		SqlDriverSettings settings,
		ConnectionSource cs,
		BoskInfo<?> bosk,
		ObjectMapper mapper,
		BoskDriver downstream
	) {
		this.settings = settings;
		this.downstream = requireNonNull(downstream);
		this.rootRef = requireNonNull(bosk.rootReference());
		this.mapper = requireNonNull(mapper);
		this.connectionSource = () -> {
			Connection result = cs.get();
			// autoCommit is an idiotic default
			result.setAutoCommit(false);
			return result;
		};
		this.S = new Statements(mapper);
		listener = Executors.newScheduledThreadPool(1, r ->
			new Thread(r, "SQL listener \""
				+ bosk.name()
				+ "\" "
				+ bosk.instanceID())
		);
	}

	public interface ConnectionSource {
		Connection get() throws SQLException;
	}

	private void listenForChanges() {
		if (!isOpen.get()) {
			LOGGER.debug("Already closed");
			return;
		}
		LOGGER.trace("Polling for changes");
		try {
			try (
				var c = connectionSource.get();
				var q = c.prepareStatement("SELECT ref, new_state, diagnostics, revision FROM bosk_changes WHERE revision > ?");
			) {
				q.setLong(1, lastChangeSubmittedDownstream.get());
				var rs = q.executeQuery();
				while (rs.next()) {
					var ref = rs.getString(1);
					var newState = rs.getString(2);
					var diagnostics = rs.getString(3);
					long changeID = rs.getLong(4);

					if (LOGGER.isTraceEnabled()) {
						record Change(String ref, String newState, String diagnostics){}
						LOGGER.trace("Received change {}: {}", changeID, new Change(ref, newState, diagnostics));
					} else {
						LOGGER.debug("Received change {}: {}", changeID, ref);
					}

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
						Object newValue;
						if (newState == null) {
							newValue = null;
						} else {
							newValue = mapper.readerFor(TypeFactory.defaultInstance().constructType(target.targetType()))
								.readValue(newState);
						}
						submitDownstream(target, newValue, changeID);
					} catch (JsonProcessingException e) {
						throw new NotYetImplementedException("Error parsing notification", e);
					} catch (InvalidTypeException e) {
						throw new NotYetImplementedException("Invalid object reference: \"" + ref + "\"", e);
					}

				}
			} catch (SQLException e) {
				throw new IllegalStateException(e);
			}
		} catch (RuntimeException e) {
			LOGGER.warn("Change processing exited unexpectedly", e);
			throw e;
		}
	}

	@SuppressWarnings("rawtypes")
	private void submitDownstream(Reference target, Object newValue, long changeID) {
		if (newValue == null) {
			LOGGER.debug("Downstream submitDeletion({})", target);
			downstream.submitDeletion(target);
		} else {
			LOGGER.debug("Downstream submitReplacement({}, ...)", target);
			downstream.submitReplacement(target, newValue);
		}
		long prev = lastChangeSubmittedDownstream.getAndSet(changeID);
		if (prev >= changeID) {
			LOGGER.error("Change ID did not increase; expected {} > {}", changeID, prev);
		}
	}

	public static <RR extends StateTreeNode> PostgresDriverFactory<RR> factory(
		SqlDriverSettings settings,
		ConnectionSource connectionSource,
		Function<BoskInfo<RR>, ObjectMapper> objectMapperFactory
	) {
		return (b, d) -> new SqlDriver(
			settings, connectionSource, b, objectMapperFactory.apply(b), d
		);
	}

	/**
	 * Best-effort cleanup, mainly meant for testing.
	 * The driver might still perform a small number of asynchronous actions after this method returns.
	 */
	public void close() {
		if (isOpen.getAndSet(false)) {
			LOGGER.debug("Closing");
			listener.shutdownNow();
		}
	}

	public interface PostgresDriverFactory<RR extends StateTreeNode> extends DriverFactory<RR> {
		@Override
		SqlDriver build(BoskInfo<RR> boskInfo, BoskDriver downstream);
	}

	@Override
	public StateTreeNode initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		// TODO: Consider a disconnected mode where we delegate downstream if something goes wrong
		LOGGER.debug("initialRoot({})", rootType);
		String json;
		try (
			var connection = connectionSource.get()
		){
			ensureTablesExist(connection);
			listener.scheduleWithFixedDelay(this::listenForChanges, 0, settings.timescaleMS(), MILLISECONDS);
			try (
				var query = connection.prepareStatement("SELECT state FROM bosk_table WHERE id='current'");
				var resultSet = S.executeQuery(connection, query)
			) {
				if (resultSet.next()) {
					json = resultSet.getString("state");
					long currentChangeID = S.latestChangeID(connection);
					S.commitTransaction(connection);
					JavaType valueType = TypeFactory.defaultInstance().constructType(rootType);
					StateTreeNode rootFromDatabase = mapper.readValue(json, valueType);
					submitDownstream(rootRef, rootFromDatabase, currentChangeID);
					return rootFromDatabase;
				} else {
					StateTreeNode root = downstream.initialRoot(rootType);
					String stateJson = mapper.writeValueAsString(root);

					S.initializeState(connection, stateJson);
					long changeID = S.insertChange(connection, rootRef, stateJson);
					S.commitTransaction(connection);
					lastChangeSubmittedDownstream.getAndSet(changeID); // Not technically "submitted"

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
				revision INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
				ref TEXT NOT NULL,
				new_state TEXT NULL,
				diagnostics TEXT NOT NULL
			);
			""");

		S.executeCommand(connection, """
			CREATE TABLE IF NOT EXISTS bosk_table (
				id CHAR(7) PRIMARY KEY NOT NULL,
				state TEXT NOT NULL
			);
			""");

		// When we say "ensure tables exist", we mean it
		connection.commit();
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		LOGGER.debug("submitReplacement({}, {})", target, newValue);
		OptionalLong revision;
		try (
			var connection = connectionSource.get()
		){
			// TODO optimization: no need to read the current state for root
			revision = replaceAndCommit(S.readState(connection), target, newValue, connection);
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
		runInBackground(revision, () -> downstream.submitReplacement(target, newValue));
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		LOGGER.debug("submitConditionalReplacement({}, {}, {}, {})", target, newValue, precondition, requiredValue);
		OptionalLong revision;
		try (
			var connection = connectionSource.get()
		){
			JsonNode state = S.readState(connection);
			if (isMatchingTextNode(precondition, requiredValue, state)) {
				revision = replaceAndCommit(state, target, newValue, connection);
			} else {
				revision = OptionalLong.empty();
			}
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
		runInBackground(revision, () -> downstream.submitConditionalReplacement(target, newValue, precondition, requiredValue));
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		LOGGER.debug("submitInitialization({}, {})", target, newValue);
		OptionalLong revision;
		try (
			var connection = connectionSource.get()
		){
			JsonNode state = S.readState(connection);
			NodeInfo node = surgeon.nodeInfo(state, target);
			if (surgeon.node(node.valueLocation(), state) == null) {
				revision = replaceAndCommit(state, target, newValue, connection);
			} else {
				revision = OptionalLong.empty();
			}
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
		runInBackground(revision, () -> downstream.submitInitialization(target, newValue));
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		LOGGER.debug("submitDeletion({})", target);
		OptionalLong revision;
		try (
			var connection = connectionSource.get()
		){
			revision = replaceAndCommit(S.readState(connection), target, null, connection);
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
		runInBackground(revision, () -> downstream.submitDeletion(target));
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		LOGGER.debug("submitConditionalDeletion({}, {}, {})", target, precondition, requiredValue);
		OptionalLong revision;
		try (
			var connection = connectionSource.get()
		){
			JsonNode state = S.readState(connection);
			if (isMatchingTextNode(precondition, requiredValue, state)) {
				revision = replaceAndCommit(state, target, null, connection);
			} else {
				revision = OptionalLong.empty();
			}
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
		runInBackground(revision, () -> downstream.submitConditionalDeletion(target, precondition, requiredValue));
	}

	private boolean isMatchingTextNode(Reference<Identifier> precondition, Identifier requiredValue, JsonNode state) {
		return surgeon.valueNode(state, precondition) instanceof TextNode text
			&& Objects.equals(text.textValue(), requiredValue.toString());
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		try (
			var connection = connectionSource.get();
		){
			long currentChangeID = S.latestChangeID(connection);
			LOGGER.debug("flush({})", currentChangeID);
			// Wait for any background tasks to finish
			background.submit(()->{}).get();
			// Wait for any pending notifications
			int retryBudget = settings.retries();
			while (lastChangeSubmittedDownstream.get() < currentChangeID) {
				if (retryBudget <= 0) {
					throw new FlushFailureException("Timed out waiting for change #" + currentChangeID);
				} else {
					LOGGER.debug("...not yet. Will try {} more times", retryBudget);
					--retryBudget;
				}
				Thread.sleep(settings.timescaleMS());
			}
		} catch (ExecutionException  | SQLException e) {
			throw new FlushFailureException(e);
		}
		downstream.flush();
	}

	/**
	 * @param state may be mutated!
	 * @param newValue if null, this is a delete
	 */
	private <T> OptionalLong replaceAndCommit(JsonNode state, Reference<T> target, T newValue, Connection connection) throws SQLException {
		NodeInfo node = surgeon.nodeInfo(state, target);
		switch (node.replacementLocation()) {
			case Root __ -> {
				if (newValue == null) {
					throw new NotYetImplementedException("Cannot delete root");
				}
				JsonNode newNode = mapper.valueToTree(newValue);
				long revision = S.insertChange(connection, target, newNode);
				S.writeState(connection, newNode);
				S.commitTransaction(connection);
				LOGGER.debug("{}: replaced root", revision);
				return OptionalLong.of(revision);
			}
			case NonexistentParent __ -> {
				// Modifying a node with a nonexistent parent is a no-op
				LOGGER.debug("--: nonexistent parent for {}", target);
				return OptionalLong.empty();
			}
			default -> {
				JsonNode newNode;
				if (newValue == null) {
					newNode = null;
					surgeon.deleteNode(node);
				} else {
					newNode = mapper.valueToTree(newValue);
					surgeon.replaceNode(node, surgeon.replacementNode(node, target.path().lastSegment(), ()->newNode));
				}
				long revision = S.insertChange(connection, target, newNode);
				S.writeState(connection, state);
				S.commitTransaction(connection);
				LOGGER.debug("--: replaced {}", target);
				return OptionalLong.of(revision);
			}
		}
	}

	private static String stringLiteral(String raw) {
		return '"' + raw.replace("\"", "\"\"") + '"';
	}

	/**
	 * Fake it till you make it
	 */
	private void runInBackground(OptionalLong revision, Runnable runnable) {
		if (true) {
			return;
		}
		if (revision.isPresent()) {
			var attributes = rootRef.diagnosticContext().getAttributes();
			background.submit(() -> {
				try (var __ = rootRef.diagnosticContext().withOnly(attributes)) {
					runnable.run();
					LOGGER.debug("submitted change {} downstream", revision.getAsLong());
					lastChangeSubmittedDownstream.getAndSet(revision.getAsLong());
				}
			});
		} else {
			LOGGER.debug("no change to submit downstream");
		}
	}

	private ObjectWriter writerFor(Type type) {
		return mapper.writerFor(TypeFactory.defaultInstance()
			.constructType(type));
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

		void commitTransaction(Connection c) throws SQLException {
			c.commit();
		}

		void initializeState(Connection c, String newValue) throws SQLException {
			executeCommand(c, """
				INSERT INTO bosk_table (id, state) VALUES ('current', ?)
				ON CONFLICT DO NOTHING;
				""", newValue);
		}

		long insertChange(Connection c, Reference<?> ref, JsonNode newValue) throws SQLException {
			try {
				return insertChange(c, ref, mapper.writeValueAsString(newValue));
			} catch (JsonProcessingException e) {
				throw new NotYetImplementedException(e);
			}
		}

		long insertChange(Connection c, Reference<?> ref, String newValue) throws SQLException {
			try (
				var q = c.prepareStatement("""
					INSERT INTO bosk_changes (ref, new_state, diagnostics) VALUES (?, ?, ?)
					RETURNING revision;
				""");
				var rs =
					 executeQuery(c, q,
						 ref.pathString(),
						 newValue,
						 mapper.writeValueAsString(ref.root().diagnosticContext().getAttributes())
					 );
			) {
				rs.next();
				return rs.getLong(1);
			} catch (JsonProcessingException e) {
				throw new NotYetImplementedException(e);
			}
		}

		JsonNode readState(Connection connection) throws SQLException {
			try (
				var s = connection.prepareStatement("SELECT state FROM bosk_table");
				var rs = executeQuery(connection, s)
			) {
				rs.next();
				return mapper.readTree(rs.getString(1));
			} catch (JsonProcessingException e) {
				throw new NotYetImplementedException(e);
			}
		}

		void writeState(Connection connection, JsonNode state) throws SQLException {
			try {
				executeCommand(connection, "UPDATE bosk_table SET state = ?", mapper.writeValueAsString(state));
			} catch (JsonProcessingException e) {
				throw new NotYetImplementedException(e);
			}
		}

		long latestChangeID(Connection connection) throws SQLException {
			try (
				var q = connection.prepareStatement(
					"SELECT max(revision) FROM bosk_changes"
				);
				var r = executeQuery(connection, q)
			) {
				r.next();
				return r.getLong(1);
			}
		}

	}

	/**
	 * The object at this path deserializes into {@code ref.value()}.
	 * Often the same as {@link #entityPath} but not necessarily always.
	 * @return jsonb path pointing to where {@code ref.value()} would be stored in the JSON structure
	 */
	private static String contentPath(Reference<?> ref) {
		ArrayList<String> steps = new ArrayList<>();
		buildFieldPath(ref, steps, true);
		return "{" + String.join(",", steps) + "}";
	}

	/**
	 * Creating the object at this path causes {@code ref} to exist;
	 * deleting it causes it not to exist.
	 * Often the same as {@link #contentPath} but not necessarily always.
	 * @return jsonb path pointing to the JSON object representing {@code ref}.
	 */
	private static String entityPath(Reference<?> ref) {
		ArrayList<String> steps = new ArrayList<>();
		buildFieldPath(ref, steps, false);
		return "{" + String.join(",", steps) + "}";
	}

	/**
	 * @param content if true, return the {@link #contentPath}; else return the {@link #entityPath}.
	 */
	private static void buildFieldPath(Reference<?> ref, ArrayList<String> steps, boolean content) {
		if (!ref.path().isEmpty()) {
			Reference<Object> enclosing = enclosingReference(ref);
			buildFieldPath(enclosing, steps, true);
			if (Listing.class.isAssignableFrom(enclosing.targetClass())) {
				steps.add(stringLiteral("entriesById"));
			} else if (SideTable.class.isAssignableFrom(enclosing.targetClass())) {
				steps.add(stringLiteral("valuesById"));
			}
			steps.add(stringLiteral(ref.path().lastSegment()));
			if (content && Catalog.class.isAssignableFrom(enclosing.targetClass())) {
				steps.add(stringLiteral("value"));
			}
		}
	}

	private static Reference<Object> enclosingReference(Reference<?> ref) {
		assert !ref.path().isEmpty();
		return ref.enclosingReference(Object.class);
	}

	private static Optional<CatalogReference<?>> directlyEnclosingCatalog(Reference<?> ref) {
		if (ref.path().isEmpty()) {
			return Optional.empty();
		}
		var enclosing = enclosingReference(ref);
		if (Catalog.class.isAssignableFrom(enclosing.targetClass())) {
			try {
				return Optional.of(enclosing.root().thenCatalog(Entity.class, enclosing.path()));
			} catch (InvalidTypeException e) {
				throw new AssertionError("Should be able to make a CatalogReference from a reference to a Catalog", e);
			}
		} else {
			return Optional.empty();
		}
	}

	private static JavaType mapValueType(Class<?> entryType) {
		return TypeFactory.defaultInstance().constructParametricType(MapValue.class, entryType);
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(SqlDriver.class);

}
