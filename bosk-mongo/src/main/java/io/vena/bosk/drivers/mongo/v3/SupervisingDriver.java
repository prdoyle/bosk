package io.vena.bosk.drivers.mongo.v3;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.vena.bosk.Bosk;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.Entity;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.drivers.mongo.BsonPlugin;
import io.vena.bosk.drivers.mongo.MongoDriver;
import io.vena.bosk.drivers.mongo.MongoDriverSettings;
import io.vena.bosk.drivers.mongo.v3.Formatter.DocumentFields;
import io.vena.bosk.drivers.mongo.v3.MappedDiagnosticContext.MDCScope;
import io.vena.bosk.exceptions.InitializationFailureException;
import io.vena.bosk.exceptions.InvalidTypeException;
import io.vena.bosk.exceptions.NotYetImplementedException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.vena.bosk.drivers.mongo.MongoDriverSettings.DatabaseFormat.SINGLE_DOC;
import static io.vena.bosk.drivers.mongo.v3.Formatter.REVISION_ONE;
import static io.vena.bosk.drivers.mongo.v3.Formatter.REVISION_ZERO;
import static io.vena.bosk.drivers.mongo.v3.MappedDiagnosticContext.setupMDC;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * This is the main driver returned to the user. This class implements the fault tolerance framework
 * in cooperation with {@link ChangeReceiver}. It's mostly exception handling code. The actual database interactions
 * used to implement the {@link BoskDriver} methods, as well as most interactions with the downstream driver,
 * are delegated to a {@link FormatDriver} object that can be swapped out as the database evolves.
 */
public class SupervisingDriver<R extends Entity> implements MongoDriver<R> {
	private final Bosk<R> bosk;
	private final ChangeReceiver receiver;
	private final MongoDriverSettings driverSettings;
	private final BsonPlugin bsonPlugin;
	private final BoskDriver<R> downstream;
	private final MongoClient mongoClient;
	private final MongoCollection<Document> collection;
	private final Listener listener;

	private volatile FormatDriver<R> formatDriver = new DisconnectedDriver<>("Driver not yet initialized");
	private volatile boolean isClosed = false;

	public SupervisingDriver(
		Bosk<R> bosk,
		MongoClientSettings clientSettings,
		MongoDriverSettings driverSettings,
		BsonPlugin bsonPlugin,
		BoskDriver<R> downstream
	) {
		try (MDCScope __ = setupMDC(bosk.name())) {
			validateMongoClientSettings(clientSettings);
			this.bosk = bosk;
			this.driverSettings = driverSettings;
			this.bsonPlugin = bsonPlugin;
			this.downstream = downstream;

			this.mongoClient = MongoClients.create(clientSettings);
			this.collection = mongoClient
				.getDatabase(driverSettings.database())
				.getCollection(COLLECTION_NAME);

			LOGGER.debug("Initializing Listener with doInitialRootTask so that it blocks until initialRoot runs");
			Type rootType = bosk.rootReference().targetType();
			this.listener = new Listener(new FutureTask<>(() -> doInitialRoot(rootType)));
			this.receiver = new ChangeReceiver(bosk.name(), listener, driverSettings, collection);
		}
	}

	private static void validateMongoClientSettings(MongoClientSettings clientSettings) {
		// We require ReadConcern and WriteConcern to be MAJORITY to ensure the Causal Consistency
		// guarantees needed to meet the requirements of the BoskDriver interface.
		// https://www.mongodb.com/docs/manual/core/causal-consistency-read-write-concerns/

		if (clientSettings.getReadConcern() != ReadConcern.MAJORITY) {
			throw new IllegalArgumentException("MongoDriver requires MongoClientSettings to specify ReadConcern.MAJORITY");
		}
		if (clientSettings.getWriteConcern() != WriteConcern.MAJORITY) {
			throw new IllegalArgumentException("MongoDriver requires MongoClientSettings to specify WriteConcern.MAJORITY");
		}
	}


	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException {
		try (MDCScope __ = beginDriverOperation("initialRoot({})", rootType)) {
			FutureTask<R> task = listener.taskRef.get();
			if (task == null) {
				throw new IllegalStateException("initialRoot has already run");
			}
			try {
				task.run();
				return task.get();
			} catch (InterruptedException e) {
				throw new IllegalStateException("Interrupted during initialization", e);
			} catch (ExecutionException e) {
				Throwable cause = e.getCause();
				if (cause instanceof DownstreamInitialRootException) {
					throw (DownstreamInitialRootException)cause;
				} else {
					throw new AssertionError("Unexpected exception during initialRoot: " + e.getClass().getSimpleName(), e);
				}
			}
		}
	}

	/**
	 * Executed on the thread that calls {@link #initialRoot}.
	 * <p>
	 * Should throw no exceptions except {@link DownstreamInitialRootException}.
	 *
	 * @throws DownstreamInitialRootException if we attempt to delegate {@link #initialRoot} to
	 * the {@link #downstream} driver and it throws an exception; this is a fatal initialization error.
	 */
	private R doInitialRoot(Type rootType) {
		R root;
		formatDriver = new DisconnectedDriver<>("Failure to compute initial root"); // Pessimism
		try {
			FormatDriver<R> detectedDriver = detectFormat();
			StateAndMetadata<R> loadedState = detectedDriver.loadAllState();
			root = loadedState.state;
			detectedDriver.onRevisionToSkip(loadedState.revision);
			formatDriver = detectedDriver;
		} catch (UninitializedCollectionException e) {
			LOGGER.debug("Database collection is uninitialized; will initialize using downstream.initialRoot", e);
			root = callDownstreamInitialRoot(rootType);
			try {
				FormatDriver<R> preferredDriver = newPreferredFormatDriver();
				initializeCollectionTransaction(root, preferredDriver);
				preferredDriver.onRevisionToSkip(REVISION_ONE); // initialRoot handles REVISION_ONE; downstream only needs to know about changes after that
				formatDriver = preferredDriver;
			} catch (RuntimeException | IOException e2) {
				LOGGER.debug("Failed to initialize database; disconnecting", e);
				formatDriver = new DisconnectedDriver<>(e2.toString());
			}
		} catch (RuntimeException | UnrecognizedFormatException | IOException e) {
			LOGGER.debug("Unable to load initial root from database; will proceed with downstream.initialRoot", e);
			formatDriver = new DisconnectedDriver<>(e.toString());
			root = callDownstreamInitialRoot(rootType);
		} finally {
			// For better or worse, we're done initialRoot. Clear taskRef so that Listener
			// enters its normal steady-state mode where onConnect events cause the state
			// to be loaded from the database and submitted downstream.
			listener.taskRef.set(null);
		}
		return root;
	}

	/**
	 * @throws DownstreamInitialRootException only
	 */
	private R callDownstreamInitialRoot(Type rootType) {
		R root;
		try {
			root = downstream.initialRoot(rootType);
		} catch (RuntimeException | InvalidTypeException | IOException | InterruptedException e) {
			LOGGER.error("Fatal error: downstream driver failed to compute initial root", e);
			throw new DownstreamInitialRootException("Fatal error: downstream driver failed to compute initial root", e);
		}
		return root;
	}

	private void initializeCollectionTransaction(R result, FormatDriver<R> newDriver) throws InitializationFailureException {
		ClientSessionOptions sessionOptions = ClientSessionOptions.builder()
			.causallyConsistent(true)
			.defaultTransactionOptions(TransactionOptions.builder()
				.writeConcern(WriteConcern.MAJORITY)
				.readConcern(ReadConcern.MAJORITY)
				.build())
			.build();
		try (ClientSession session = mongoClient.startSession(sessionOptions)) {
			try {
				newDriver.initializeCollection(new StateAndMetadata<>(result, REVISION_ZERO));
			} finally {
				if (session.hasActiveTransaction()) {
					session.abortTransaction();
				}
			}
		}
	}

	private void refurbishTransaction() throws IOException {
		ClientSessionOptions sessionOptions = ClientSessionOptions.builder()
			.causallyConsistent(true)
			.defaultTransactionOptions(TransactionOptions.builder()
				.writeConcern(WriteConcern.MAJORITY)
				.readConcern(ReadConcern.MAJORITY)
				.build())
			.build();
		try (ClientSession session = mongoClient.startSession(sessionOptions)) {
			try {
				// Design note: this operation shouldn't do any special coordination with
				// the receiver/listener system, because other replicas won't.
				// That system needs to cope with a refurbish operations without any help.
				session.startTransaction();
				StateAndMetadata<R> result = formatDriver.loadAllState();
				FormatDriver<R> newFormatDriver = newPreferredFormatDriver();
				collection.deleteMany(new BsonDocument());
				newFormatDriver.initializeCollection(result);
				session.commitTransaction();
				formatDriver = newFormatDriver;
			} catch (UninitializedCollectionException e) {
				throw new IOException("Unable to refurbish uninitialized database collection", e);
			} finally {
				if (session.hasActiveTransaction()) {
					session.abortTransaction();
				}
			}
		}
	}
	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		try (MDCScope __ = beginDriverOperation("submitReplacement({})", target)) {
			formatDriver.submitReplacement(target, newValue);
		}
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		try (MDCScope __ = beginDriverOperation("submitConditionalReplacement({}, {}={})", target, precondition, requiredValue)) {
			formatDriver.submitConditionalReplacement(target, newValue, precondition, requiredValue);
		}
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		try (MDCScope __ = beginDriverOperation("submitInitialization({})", target)) {
			formatDriver.submitInitialization(target, newValue);
		}
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		try (MDCScope __ = beginDriverOperation("submitDeletion({})", target)) {
			formatDriver.submitDeletion(target);
		}
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		try (MDCScope __ = beginDriverOperation("submitConditionalDeletion({}, {}={})", target, precondition, requiredValue)) {
			formatDriver.submitConditionalDeletion(target, precondition, requiredValue);
		}
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		try (MDCScope __ = beginDriverOperation("flush")) {
			formatDriver.flush();
		}
	}

	@Override
	public void refurbish() throws IOException {
		try (MDCScope __ = beginDriverOperation("refurbish")) {
			refurbishTransaction();
		}
	}

	@Override
	public void close() {
		isClosed = true;
		receiver.close();
		throw new NotYetImplementedException();
	}

	/**
	 * Contains logic that runs on the {@link ChangeReceiver}'s background thread
	 * to update {@link SupervisingDriver}'s state in response to various occurrences.
	 */
	private class Listener implements ChangeListener {
		final AtomicReference<FutureTask<R>> taskRef;

		private Listener(FutureTask<R> initialRootAction) {
			this.taskRef = new AtomicReference<>(initialRootAction);
		}

		@Override
		public void onConnect() throws
			UnrecognizedFormatException,
			UninitializedCollectionException,
			InterruptedException,
			IOException,
			InitialRootException,
			TimeoutException
		{
			FutureTask<R> initialRootAction = this.taskRef.get();
			if (initialRootAction != null) {
				LOGGER.debug("Waiting for initialRoot action");
				try {
					initialRootAction.get(5 * driverSettings.recoveryPollingMS(), MILLISECONDS);
					LOGGER.debug("initialRoot action completed successfully");
				} catch (ExecutionException e) {
					LOGGER.debug("initialRoot action failed", e);
					throw new InitialRootException(e.getCause());
				}
			} else {
				LOGGER.debug("Loading database state to submit to downstream driver");
				FormatDriver<R> newDriver = detectFormat();
				StateAndMetadata<R> loadedState = newDriver.loadAllState();
				downstream.submitReplacement(bosk.rootReference(), loadedState.state);
				formatDriver = newDriver;
			}
		}

		@Override
		public void onEvent(ChangeStreamDocument<Document> event) throws UnprocessableEventException {
			formatDriver.onEvent(event);
		}

		@Override
		public void onDisconnect(Exception e) {
			formatDriver = new DisconnectedDriver<>(e.toString());
		}
	}

	private FormatDriver<R> newPreferredFormatDriver() {
		if (driverSettings.preferredDatabaseFormat() == SINGLE_DOC) {
			return newSingleDocFormatDriver(REVISION_ZERO.longValue());
		} else {
			throw new AssertionError("Unknown database format setting: " + driverSettings.preferredDatabaseFormat());
		}
	}

	private FormatDriver<R> detectFormat() throws UninitializedCollectionException, UnrecognizedFormatException {
		FindIterable<Document> result = collection.find(new BsonDocument("_id", SingleDocFormatDriver.DOCUMENT_ID));
		try (MongoCursor<Document> cursor = result.cursor()) {
			if (cursor.hasNext()) {
				Long revision = cursor
					.next()
					.get(DocumentFields.revision.name(), 0L);
				return newSingleDocFormatDriver(revision);
			} else {
				throw new UninitializedCollectionException("Document doesn't exist");
			}
		}
	}

	private SingleDocFormatDriver<R> newSingleDocFormatDriver(long revisionAlreadySeen) {
		return new SingleDocFormatDriver<>(
			bosk,
			collection,
			driverSettings,
			bsonPlugin,
			new FlushLock(driverSettings, revisionAlreadySeen),
			downstream);
	}


	private MDCScope beginDriverOperation(String description, Object... args) {
		if (isClosed) {
			throw new IllegalStateException("Driver is closed");
		}
		MDCScope ex = setupMDC(bosk.name());
		LOGGER.debug(description, args);
		return ex;
	}

	public static final String COLLECTION_NAME = "boskCollection";
	private static final Logger LOGGER = LoggerFactory.getLogger(SupervisingDriver.class);
}
