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
import java.util.concurrent.Semaphore;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.vena.bosk.drivers.mongo.MongoDriverSettings.DatabaseFormat.SINGLE_DOC;
import static io.vena.bosk.drivers.mongo.v3.Formatter.REVISION_ZERO;
import static io.vena.bosk.drivers.mongo.v3.MappedDiagnosticContext.setupMDC;

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
		validateMongoClientSettings(clientSettings);
		this.bosk = bosk;
		this.driverSettings = driverSettings;
		this.bsonPlugin = bsonPlugin;
		this.downstream = downstream;

		this.mongoClient = MongoClients.create(clientSettings);
		this.collection = mongoClient
			.getDatabase(driverSettings.database())
			.getCollection(COLLECTION_NAME);

		this.listener = new Listener();
		this.receiver = new ChangeReceiver(bosk.name(), listener, driverSettings, collection);
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
		R root;
		try {
			FormatDriver<R> newDriver = detectFormat();
			StateAndMetadata<R> loadedState = newDriver.loadAllState();
			root = loadedState.state;
			formatDriver = newDriver;
		} catch (UnrecognizedFormatException|IOException e) {
			throw new NotYetImplementedException("Should disconnect", e);
		} catch (UninitializedCollectionException e) {
			try {
				root = downstream.initialRoot(rootType);
				FormatDriver<R> newDriver = newPreferredFormatDriver();
				doInitialization(root, newDriver);
				formatDriver = newDriver;
			} catch (IOException | InterruptedException e2) {
				throw new NotYetImplementedException("Should disconnect", e2);
			}
		} finally {
			// Our attempt at clean initialization is done, for better or worse.
			// Release the listener to allow change event processing to proceed.
			listener.semaphore.release();
		}
		return root;
	}

	private void doInitialization(R result, FormatDriver<R> newDriver) throws InitializationFailureException {
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
		throw new NotYetImplementedException();
	}

	@Override
	public void close() {
		isClosed = true;
		throw new NotYetImplementedException();
	}

	private class Listener implements ChangeListener {
		final Semaphore semaphore = new Semaphore(0);

		@Override
		public void onConnect() throws
			UnrecognizedFormatException,
			UninitializedCollectionException,
			InterruptedException,
			IOException
		{
			if (!semaphore.tryAcquire()) {
				LOGGER.debug("Waiting for initialRoot");
				semaphore.acquire();
				LOGGER.debug("initialRoot finished; connection is complete");
				return;
			}
			FormatDriver<R> newDriver = detectFormat();
			StateAndMetadata<R> loadedState = newDriver.loadAllState();
			downstream.submitReplacement(bosk.rootReference(), loadedState.state);
			formatDriver = newDriver;
			throw new NotYetImplementedException();
		}

		@Override
		public void onEvent(ChangeStreamDocument<Document> event) throws UnprocessableEventException {
			formatDriver.onEvent(event);
		}

		@Override
		public void onDisconnect(Exception e) {
			throw new NotYetImplementedException();
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
