package works.bosk.drivers.sql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.jooq.Record;
import org.jooq.TableField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import works.bosk.BoskDriver;
import works.bosk.BoskInfo;
import works.bosk.DriverFactory;
import works.bosk.Identifier;
import works.bosk.MapValue;
import works.bosk.Path;
import works.bosk.Reference;
import works.bosk.RootReference;
import works.bosk.StateTreeNode;
import works.bosk.drivers.sql.schema.BoskTable;
import works.bosk.drivers.sql.schema.ChangesTable;
import works.bosk.drivers.sql.schema.Schema;
import works.bosk.exceptions.FlushFailureException;
import works.bosk.exceptions.InvalidTypeException;
import works.bosk.exceptions.NotYetImplementedException;
import works.bosk.jackson.JsonNodeSurgeon;
import works.bosk.jackson.JsonNodeSurgeon.NodeInfo;
import works.bosk.jackson.JsonNodeSurgeon.NodeLocation.NonexistentParent;
import works.bosk.jackson.JsonNodeSurgeon.NodeLocation.Root;

import static java.lang.Math.min;
import static java.lang.Math.multiplyExact;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.primaryKey;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.using;

public class SqlDriver implements BoskDriver {
	final SqlDriverSettings settings;
	final BoskDriver downstream;
	final RootReference<?> rootRef;
	final ConnectionSource connectionSource;
	final ObjectMapper mapper;
	final JsonNodeSurgeon surgeon = new JsonNodeSurgeon();

	final AtomicBoolean isOpen = new AtomicBoolean(true);

	// jOOQ references
	final TableField<Record, String> DIAGNOSTICS;
	final ChangesTable CHANGES;
	final TableField<Record, Long> REVISION;
	final TableField<Record, String> REF;
	final TableField<Record, String> NEW_STATE;
	final TableField<Record, String> STATE;
	final BoskTable BOSK;
	final TableField<Record, String> ID;

	final ScheduledExecutorService listener;
	private final Schema schema;

	private volatile String epoch;

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

		// Set up jOOQ references
		schema = new Schema(name(settings.schemaName()));
		REF = schema.REF;
		NEW_STATE = schema.NEW_STATE;
		REVISION = schema.REVISION;
		CHANGES = schema.CHANGES;
		DIAGNOSTICS = schema.DIAGNOSTICS;
		STATE = schema.STATE;
		BOSK = schema.BOSK;
		ID = schema.ID;

		// Kick off listener thread
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
			try (var c = connectionSource.get()) {
				var rs = using(c)
					.select(REF, NEW_STATE, DIAGNOSTICS, CHANGES.EPOCH, REVISION)
					.from(CHANGES)
					.where(REVISION.gt(lastChangeSubmittedDownstream.get()))
					.fetch();
				for (var r: rs) {
					var ref = r.get(REF);
					var newState = r.get(NEW_STATE);
					var diagnostics = r.get(DIAGNOSTICS);
					String epoch = r.get(CHANGES.EPOCH);
					long changeID = r.get(REVISION);

					if (!epoch.equals(this.epoch)) {
						throw new NotYetImplementedException("Epoch changed!");
					}

					if (LOGGER.isTraceEnabled()) {
						record Change(String ref, String newState, String diagnostics){}
						LOGGER.trace("Received change {}: {}", changeID, new Change(ref, newState, diagnostics));
					} else {
						LOGGER.debug("Received change {}: {} (diagnostics: {})", changeID, ref, diagnostics);
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
				throw new NotYetImplementedException(e);
			}
		} catch (RuntimeException e) {
			LOGGER.warn("Change processing exited unexpectedly", e);
			throw e;
		}
	}

	@SuppressWarnings({"rawtypes","unchecked"})
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
			// TODO: Re-initialize to recover
			LOGGER.debug("Change ID did not increase from {} to {}", prev, changeID);
			throw new NotYetImplementedException("Change ID did not increase: changed from " + prev + " to " + changeID);
		} else {
			LOGGER.debug("Change ID increased from {} to {}", prev, changeID);
		}
	}

	public static <RR extends StateTreeNode> SqlDriverFactory<RR> factory(
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
			if (LOGGER.isTraceEnabled()) {
				try (var c = connectionSource.get()) {
					var rs = using(c).select(CHANGES.asterisk()).from(CHANGES).fetch();
					LOGGER.trace("\n=== Final change table contents ===\n{}", rs);
				} catch (SQLException e) {
					throw new NotYetImplementedException(e);
				}
			}
			listener.shutdownNow();
		}
	}

	public interface SqlDriverFactory<RR extends StateTreeNode> extends DriverFactory<RR> {
		@Override
		SqlDriver build(BoskInfo<RR> boskInfo, BoskDriver downstream);
	}

	@Override
	public StateTreeNode initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		// TODO: Consider a disconnected mode where we delegate downstream if something goes wrong
		LOGGER.debug("initialRoot({})", rootType);
		try (
			var connection = connectionSource.get()
		){
			// TODO: It seems wrong to schedule the listener loop here. It should be in the constructor.
			ensureTablesExist(connection);
			StateTreeNode result;
			record Row(String state, String epoch){}
			var row = using(connection)
				.select(STATE, BOSK.EPOCH)
				.from(BOSK)
				.where(ID.eq("current"))
				.fetchOneInto(Row.class);
			if (row == null) {
				LOGGER.debug("No current state; initializing {} table from downstream", BOSK);
				this.epoch = UUID.randomUUID().toString();
				result = downstream.initialRoot(rootType);
				String stateJson = mapper.writeValueAsString(result);

				using(connection)
					.insertInto(BOSK).columns(ID, STATE, BOSK.EPOCH)
					.values("current", stateJson, epoch)
					.onConflictDoNothing()
					.execute();
				long changeID = insertChange(connection, rootRef, stateJson);
				connection.commit();
				lastChangeSubmittedDownstream.getAndSet(changeID); // Not technically "submitted"
			} else {
				LOGGER.debug("Database state exists; initializing downstream from {} table", BOSK);
				this.epoch = row.epoch;
				long currentChangeID = latestChangeID(connection);
				JavaType valueType = TypeFactory.defaultInstance().constructType(rootType);
				result = mapper.readValue(row.state, valueType);
				connection.commit();
				submitDownstream(rootRef, result, currentChangeID);
			}
			listener.scheduleWithFixedDelay(this::listenForChanges, 0, settings.timescaleMS(), MILLISECONDS);
			return result;
		} catch (SQLException e) {
			throw new NotYetImplementedException(e);
		}
	}

	private void ensureTablesExist(Connection connection) throws SQLException {
		using(connection)
			.createSchemaIfNotExists(schema)
			.execute();

		using(connection)
			.createTableIfNotExists(CHANGES)
			.columns(CHANGES.EPOCH, REVISION, REF, NEW_STATE, DIAGNOSTICS)
			.constraints(primaryKey(REVISION))
			.execute();

		using(connection)
			.createTableIfNotExists(BOSK)
			.columns(ID, BOSK.EPOCH, STATE)
			.constraints(primaryKey(ID))
			.execute();

		// When we say "ensure tables exist", we mean it
		connection.commit();
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		LOGGER.debug("submitReplacement({}, {})", target, newValue);
		try (
			var connection = connectionSource.get()
		){
			// TODO optimization: no need to read the current state for root
			replaceAndCommit(readState(connection), target, newValue, connection);
		} catch (SQLException e) {
			throw new NotYetImplementedException(e);
		}
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		LOGGER.debug("submitConditionalReplacement({}, {}, {}, {})", target, newValue, precondition, requiredValue);
		try (
			var connection = connectionSource.get()
		){
			JsonNode state = readState(connection);
			if (isMatchingTextNode(precondition, requiredValue, state)) {
				replaceAndCommit(state, target, newValue, connection);
			}
		} catch (SQLException e) {
			throw new NotYetImplementedException(e);
		}
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		LOGGER.debug("submitInitialization({}, {})", target, newValue);
		try (
			var connection = connectionSource.get()
		){
			JsonNode state = readState(connection);
			NodeInfo node = surgeon.nodeInfo(state, target);
			if (surgeon.node(node.valueLocation(), state) == null) {
				replaceAndCommit(state, target, newValue, connection);
			}
		} catch (SQLException e) {
			throw new NotYetImplementedException(e);
		}
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		LOGGER.debug("submitDeletion({})", target);
		try (
			var connection = connectionSource.get()
		){
			replaceAndCommit(readState(connection), target, null, connection);
		} catch (SQLException e) {
			throw new NotYetImplementedException(e);
		}
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		LOGGER.debug("submitConditionalDeletion({}, {}, {})", target, precondition, requiredValue);
		try (
			var connection = connectionSource.get()
		){
			JsonNode state = readState(connection);
			if (isMatchingTextNode(precondition, requiredValue, state)) {
				replaceAndCommit(state, target, null, connection);
			}
		} catch (SQLException e) {
			throw new NotYetImplementedException(e);
		}
	}

	private boolean isMatchingTextNode(Reference<Identifier> precondition, Identifier requiredValue, JsonNode state) {
		return surgeon.valueNode(state, precondition) instanceof TextNode text
			&& Objects.equals(text.textValue(), requiredValue.toString());
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		try (
			var connection = connectionSource.get()
		){
			long currentChangeID = latestChangeID(connection);
			LOGGER.debug("flush({})", currentChangeID);

			// Exponential backoff starting from timescaleMS
			long sleepTime = settings.timescaleMS();
			long sleepDeadline = currentTimeMillis() + multiplyExact(sleepTime, settings.patienceFactor());
			while (lastChangeSubmittedDownstream.get() < currentChangeID) {
				long sleepBudget = sleepDeadline - currentTimeMillis();
				if (sleepBudget <= 0) {
					throw new FlushFailureException("Timed out waiting for change #" + currentChangeID);
				}
				LOGGER.debug("Will retry");
				Thread.sleep(min(sleepBudget, sleepTime));
				sleepTime *= 2;
			}
		} catch (SQLException e) {
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
				String json;
				try {
					json = mapper.writeValueAsString(newValue);
				} catch (JsonProcessingException e) {
					throw new NotYetImplementedException(e);
				}
				long revision = insertChange(connection, target, json);
				using(connection)
					.update(BOSK)
					.set(STATE, json)
					.execute();
				connection.commit();
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
				String nodeJson, stateJson;
				try {
					nodeJson = mapper.writeValueAsString(newNode);
					stateJson = mapper.writeValueAsString(state);
				} catch (JsonProcessingException e) {
					throw new NotYetImplementedException(e);
				}
				long revision = insertChange(connection, target, nodeJson);
				using(connection)
					.update(BOSK)
					.set(STATE, stateJson)
					.execute();
				connection.commit();
				LOGGER.debug("--: replaced {}", target);
				return OptionalLong.of(revision);
			}
		}
	}

	private long insertChange(Connection c, Reference<?> ref, String newValue) {
		try {
			return using(c)
				.insertInto(CHANGES).columns(CHANGES.EPOCH, REF, NEW_STATE, DIAGNOSTICS)
				.values(epoch, ref.pathString(), newValue, mapper.writeValueAsString(ref.root().diagnosticContext().getAttributes()))
				.returning(REVISION)
				.fetchOptional(REVISION)
				.orElseThrow(()->new NotYetImplementedException("No change inserted"));
		} catch (JsonProcessingException e) {
			throw new NotYetImplementedException(e);
		}
	}

	private JsonNode readState(Connection connection) {
		var json = using(connection)
			.select(STATE)
			.from(BOSK)
			.fetchOptional(STATE)
			.orElseThrow(()->new NotYetImplementedException("No state found"));
		try {
			return mapper.readTree(json);
		} catch (JsonProcessingException e) {
			throw new NotYetImplementedException(e);
		}
	}

	private long latestChangeID(Connection connection) {
		record Row(String epoch, Long revision){}
		var result = using(connection)
			.select(CHANGES.EPOCH, REVISION)
			.from(CHANGES)
			.where(REVISION.eq(select(max(REVISION)).from(CHANGES)))
			.fetchOneInto(Row.class);
		if (result == null) {
			// No changes yet; return something less than the first change ID
			return 0L; // TODO: Are we sure auto-increment always generates numbers strictly greater than zero?
		} else {
			if (!result.epoch.equals(this.epoch)) {
				throw new NotYetImplementedException("Epoch mismatch");
			}
			return result.revision;
		}
	}

	private static JavaType mapValueType(Class<?> entryType) {
		return TypeFactory.defaultInstance().constructParametricType(MapValue.class, entryType);
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(SqlDriver.class);
}
