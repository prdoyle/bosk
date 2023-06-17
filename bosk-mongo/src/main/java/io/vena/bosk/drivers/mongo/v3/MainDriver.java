package io.vena.bosk.drivers.mongo.v3;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import io.vena.bosk.Entity;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.drivers.mongo.MongoDriver;
import io.vena.bosk.drivers.mongo.MongoDriverSettings;
import io.vena.bosk.drivers.mongo.v2.ReceiverInitializationException;
import io.vena.bosk.exceptions.InvalidTypeException;
import io.vena.bosk.exceptions.TunneledCheckedException;
import java.io.IOException;
import java.lang.reflect.Type;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.vena.bosk.drivers.mongo.v2.MainDriver.COLLECTION_NAME;
import static io.vena.bosk.drivers.mongo.v3.SingleDocFormatDriver.DOCUMENT_ID;

public class MainDriver<R extends Entity> implements MongoDriver<R>, ResettableDriver<R> {
	private final ChangeEventAgent agent;
	private final MongoCollection<Document> collection;
	private final MongoClient mongoClient;
	private volatile FormatDriver<R> formatDriver;

	public MainDriver(MongoDriverSettings driverSettings, MongoClientSettings clientSettings) {
		this.agent = new ChangeEventAgent(this, driverSettings);
		this.mongoClient = MongoClients.create(clientSettings);
		this.collection = mongoClient
			.getDatabase(driverSettings.database())
			.getCollection(COLLECTION_NAME);
	}

	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		return null;
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {

	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {

	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {

	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {

	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {

	}

	@Override
	public void flush() throws IOException, InterruptedException {

	}

	@Override
	public void refurbish() throws IOException {

	}

	@Override
	public void close() {

	}

	@Override
	public R initializeReplication() {
		LOGGER.debug("Initializing replication");
		try {
			formatDriver.close();
			formatDriver = new DisconnectedDriver<>("Driver initialization failed"); // Fallback in case initialization fails
			if (receiver.initialize(new MainDriver.Listener())) {
				FormatDriver<R> newDriver = detectFormat();
				StateAndMetadata<R> result = newDriver.loadAllState();
				newDriver.onRevisionToSkip(result.revision);
				formatDriver = newDriver;
				return result.state;
			} else {
				LOGGER.warn("Unable to fetch resume token; disconnecting");
				formatDriver = new DisconnectedDriver<>("Unable to fetch resume token");
				return null;
			}
		} catch (ReceiverInitializationException | IOException | RuntimeException e) {
			LOGGER.warn("Failed to initialize replication", e);
			formatDriver = new DisconnectedDriver<>(e.toString());
			throw new TunneledCheckedException(e);
		} finally {
			// Clearing the map entry here allows the next initialization task to be created
			// now that this one has completed
			initializationInProgress.set(null);
		}
	}

	private FormatDriver<R> detectFormat() throws io.vena.bosk.drivers.mongo.v3.UnrecognizedFormatException, UninitializedCollectionException {
		FindIterable<Document> result = collection.find(new BsonDocument("_id", DOCUMENT_ID));
		try (MongoCursor<Document> cursor = result.cursor()) {
			if (cursor.hasNext()) {
				Long revision = cursor
					.next()
					.get(io.vena.bosk.drivers.mongo.v3.Formatter.DocumentFields.revision.name(), 0L);
				return newSingleDocFormatDriver(revision);
			} else {
				throw new io.vena.bosk.drivers.mongo.v3.UninitializedCollectionException("Document doesn't exist");
			}
		}
	}

	private io.vena.bosk.drivers.mongo.v3.SingleDocFormatDriver<R> newSingleDocFormatDriver(long revisionAlreadySeen) {
		return new io.vena.bosk.drivers.mongo.v3.SingleDocFormatDriver<>(
			bosk,
			collection,
			driverSettings,
			bsonPlugin,
			new io.vena.bosk.drivers.mongo.v3.FlushLock(driverSettings, revisionAlreadySeen),
			downstream);
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(MainDriver.class);

}
