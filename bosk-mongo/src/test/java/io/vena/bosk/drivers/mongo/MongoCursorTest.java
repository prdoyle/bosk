package io.vena.bosk.drivers.mongo;

import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertTrue;

//@UsesMongoService
public class MongoCursorTest {
	final MongoService mongoService = new MongoService();
	MongoDatabase database = mongoService.client().getDatabase(DATABASE);
	MongoCollection<Document> collection = database.getCollection(COLLECTION);

	@BeforeEach
	void setup() {
		collection
			.insertOne(new Document("field", 0L));
	}

	@Test
	void testChangeStreams() throws InterruptedException, BrokenBarrierException {
		// This is used to kick off the two threads at the same time to make race conditions more likely
		CyclicBarrier barrier = new CyclicBarrier(2);

		// This is used to signal when the Opener thread observes the field getting updated
		Semaphore semaphore = new Semaphore(0);

		Thread cursorOpener = new Thread(() -> {
			Thread.currentThread().setName("Opener");
			while (true) {
				setFieldTo(0);
				try {
					barrier.await();
					LOGGER.info("--- Starting Opener");
				} catch (InterruptedException | BrokenBarrierException e) {
					LOGGER.info("Exiting");
					return;
				}
//				LOGGER.info("Opening initial cursor");
				ChangeStreamDocument<Document> initialEvent = null;
//				BsonDocument resumeToken;
//				try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection
//					.watch()
//					.maxAwaitTime(20, MILLISECONDS)
//					.cursor()
//				) {
//					initialEvent = cursor.tryNext();
//					resumeToken = cursor.getResumeToken();
//				}
				LOGGER.info("Opening cursor");
				try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection
					.watch()
//					.startAfter(resumeToken)
					.maxAwaitTime(500, MILLISECONDS)
					.cursor()
				) {
					long startingValue = readField();
					if (startingValue == 1) {
						LOGGER.info("Observed {} already; will start over", startingValue);
						semaphore.release();
						continue;
					}
					LOGGER.info("Observed {}; waiting for change event", startingValue);
					if (initialEvent == null) {
						LOGGER.info("Waiting for change event");
						cursor.next();
						LOGGER.info("Change event received");
					} else {
						LOGGER.info("Saw initial event");
					}
					semaphore.release();
				}
			}
		});
		cursorOpener.start();

		Thread.currentThread().setName("Setter");
		try {
			long stopTime = 30_000 + System.currentTimeMillis();
			while (System.currentTimeMillis() < stopTime) {
				try {
					barrier.await();
					// This makes it pass
//					LOGGER.info("Sleeping");
//					Thread.sleep(100);
					LOGGER.info("--- Starting Setter");
				} catch (InterruptedException | BrokenBarrierException e) {
					return;
				}

				LOGGER.info("Setting field to 1");
				setFieldTo(1);
				long value = readField();
				LOGGER.info("Observed value {}", value);
				LOGGER.info("Waiting for semaphore");

				// 5 seconds should be plenty
				boolean success = semaphore.tryAcquire(5, SECONDS);
				assertTrue(success);
			}
		} finally {
			cursorOpener.interrupt();
			cursorOpener.join();
		}
	}

	private long readField() {
		try (MongoCursor<Document> cursor = collection.find(new BsonDocument()).cursor()) {
			return cursor.next().getLong("field");
		}
	}

	private void setFieldTo(long newValue) {
		LOGGER.info("setFieldTo({})", newValue);
		collection.updateOne(new BsonDocument(),
			new BsonDocument("$set", new BsonDocument("field", new BsonInt64(newValue))));
	}

	private MongoChangeStreamCursor<ChangeStreamDocument<Document>> openCursor() {
		return collection
			.watch()
			.maxAwaitTime(500, MILLISECONDS)
			.cursor();
	}

	@AfterEach
	void teardown() {
		database.drop();
	}

	private static final String DATABASE = "MongoCursorTest_DB";
	private static final String COLLECTION = "MongoCursorTest_C";

	private static final Logger LOGGER = LoggerFactory.getLogger(MongoCursorTest.class);
}
