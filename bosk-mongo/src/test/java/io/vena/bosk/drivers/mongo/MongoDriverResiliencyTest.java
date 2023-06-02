package io.vena.bosk.drivers.mongo;

import io.vena.bosk.Bosk;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.Listing;
import io.vena.bosk.drivers.mongo.Formatter.DocumentFields;
import io.vena.bosk.drivers.state.TestEntity;
import io.vena.bosk.exceptions.FlushFailureException;
import io.vena.bosk.exceptions.InvalidTypeException;
import io.vena.bosk.junit.ParametersByName;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import lombok.var;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.junit.jupiter.api.Disabled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.vena.bosk.ListingEntry.LISTING_ENTRY;
import static io.vena.bosk.drivers.mongo.MongoDriverSettings.ImplementationKind.RESILIENT;
import static io.vena.bosk.drivers.mongo.v2.MainDriver.COLLECTION_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A set of tests that only work with {@link io.vena.bosk.drivers.mongo.MongoDriverSettings.ImplementationKind#RESILIENT}
 */
public class MongoDriverResiliencyTest extends AbstractMongoDriverTest {
	@ParametersByName
	public MongoDriverResiliencyTest(MongoDriverSettings.MongoDriverSettingsBuilder driverSettings) {
		super(driverSettings);
	}

	@SuppressWarnings("unused")
	static Stream<MongoDriverSettings.MongoDriverSettingsBuilder> driverSettings() {
		return Stream.of(
			MongoDriverSettings.builder()
				.database("boskResiliencyTestDB_" + dbCounter.incrementAndGet())
				.implementationKind(RESILIENT)
				.testing(MongoDriverSettings.Testing.builder()
					.eventDelayMS(100)
					.build())
		);
	}

	@ParametersByName
	@DisruptsMongoService
	void initialOutage_recovers() throws InvalidTypeException, InterruptedException, IOException {
		// Set up the database contents to be different from initialRoot
		TestEntity initialState = initializeDatabase("distinctive string", false);

		mongoService.proxy().setConnectionCut(true);

		Bosk<TestEntity> bosk = new Bosk<TestEntity>("Test bosk", TestEntity.class, this::initialRoot, driverFactory);
		MongoDriverSpecialTest.Refs refs = bosk.buildReferences(MongoDriverSpecialTest.Refs.class);
		BoskDriver<TestEntity> driver = bosk.driver();
		TestEntity defaultState = initialRoot(bosk);

		try (var __ = bosk.readContext()) {
			assertEquals(defaultState, bosk.rootReference().value(),
				"Uses default state if database is unavailable");
		}

		assertThrows(FlushFailureException.class, driver::flush,
			"Flush disallowed during outage");
		assertThrows(Exception.class, () -> driver.submitReplacement(bosk.rootReference(), initialRoot(bosk)),
			"Updates disallowed during outage");

		mongoService.proxy().setConnectionCut(false);

		driver.flush();
		try (var __ = bosk.readContext()) {
			assertEquals(initialState, bosk.rootReference().value(),
				"Updates to database state once it reconnects");
		}

		// Make a change to the bosk and verify that it gets through
		driver.submitReplacement(refs.listingEntry(entity123), LISTING_ENTRY);
		TestEntity expected = initialRoot(bosk)
			.withString("distinctive string")
			.withListing(Listing.of(refs.catalog(), entity123));


		driver.flush();
		try (@SuppressWarnings("unused") Bosk<?>.ReadContext readContext = bosk.readContext()) {
			assertEquals(expected, bosk.rootReference().value());
		}
	}

	@ParametersByName
	@UsesMongoService
	void databaseDeleted_recovers() throws InvalidTypeException, InterruptedException, IOException {
		testRecoveryAfterDeletion(false, () -> {
			LOGGER.debug("Drop database");
			mongoService.client()
				.getDatabase(driverSettings.database())
				.drop();
		});
	}

	@ParametersByName
	@UsesMongoService
	void collectionDeleted_recovers() throws InvalidTypeException, InterruptedException, IOException {
		testRecoveryAfterDeletion(false, () -> {
			LOGGER.debug("Drop collection");
			mongoService.client()
				.getDatabase(driverSettings.database())
				.getCollection(COLLECTION_NAME)
				.drop();
		});
	}

	@ParametersByName
	@UsesMongoService
	@Disabled("Document deletion currently does not initiate recovery, because deletion also happens in refurbish; but this means it also can't cope with the revision field decreasing")
	void documentDeleted_recovers() throws InvalidTypeException, InterruptedException, IOException {
		testRecoveryAfterDeletion(false, () -> {
			LOGGER.debug("Delete document");
			mongoService.client()
				.getDatabase(driverSettings.database())
				.getCollection(COLLECTION_NAME)
				.deleteMany(new BsonDocument());
		});
	}

	@ParametersByName
	@UsesMongoService
	void revisionDeleted_recovers() throws InvalidTypeException, InterruptedException, IOException {
		testRecoveryAfterDeletion(true, () -> {
			LOGGER.debug("Delete revision");
			mongoService.client()
				.getDatabase(driverSettings.database())
				.getCollection(COLLECTION_NAME)
				.updateOne(
					new BsonDocument(),
					new BsonDocument("$unset", new BsonDocument(DocumentFields.revision.name(), new BsonNull())) // Value is ignored
				);
		});
	}

	@ParametersByName
	@UsesMongoService
	@Disabled("Situations where the revision number goes backward currently don't work")
	void revisionDecreased_recovers() throws InvalidTypeException, InterruptedException, IOException {
		LOGGER.debug("Decrease revision");
		mongoService.client()
			.getDatabase(driverSettings.database())
			.getCollection(COLLECTION_NAME)
			.updateOne(
				new BsonDocument(),
				new BsonDocument("$dec", new BsonDocument(DocumentFields.revision.name(), new BsonInt64(1)))
			);
	}

	private TestEntity initializeDatabase(String distinctiveString, boolean doRefurbish) throws IOException, InterruptedException, InvalidTypeException {
		Bosk<TestEntity> prepBosk = new Bosk<TestEntity>(
			"Prep bosk " + prepBoskCounter.incrementAndGet(),
			TestEntity.class,
			bosk -> initialRoot(bosk).withString(distinctiveString),
			driverFactory);
		MongoDriver<TestEntity> driver = (MongoDriver<TestEntity>) prepBosk.driver();
		if (doRefurbish) {
			driver.refurbish();
		}
		driver.flush();
		driver.close();

		return initialRoot(prepBosk).withString(distinctiveString);
	}

	private void testRecoveryAfterDeletion(boolean doRefurbish, Runnable action) throws IOException, InterruptedException, InvalidTypeException {
		LOGGER.debug("Setup database to beforeState");
		TestEntity beforeState = initializeDatabase("before deletion", false);

		Bosk<TestEntity> bosk = new Bosk<TestEntity>("Test bosk", TestEntity.class, this::initialRoot, driverFactory);
		try (var __ = bosk.readContext()) {
			assertEquals(beforeState, bosk.rootReference().value());
		}

		action.run();

		LOGGER.debug("Ensure flush throws");
		assertThrows(FlushFailureException.class, () -> bosk.driver().flush());
		try (var __ = bosk.readContext()) {
			assertEquals(beforeState, bosk.rootReference().value());
		}

		LOGGER.debug("Setup database to afterState");
		TestEntity afterState = initializeDatabase("after deletion", doRefurbish);

		LOGGER.debug("Ensure flush works");
		bosk.driver().flush();
		try (var __ = bosk.readContext()) {
			assertEquals(afterState, bosk.rootReference().value());
		}
	}

	private static final AtomicInteger dbCounter = new AtomicInteger(0);
	private static final AtomicInteger prepBoskCounter = new AtomicInteger(0);

	private static final Logger LOGGER = LoggerFactory.getLogger(MongoDriverResiliencyTest.class);
}
