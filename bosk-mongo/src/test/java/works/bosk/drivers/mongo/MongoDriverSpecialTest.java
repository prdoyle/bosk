package works.bosk.drivers.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Stream;
import lombok.With;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import works.bosk.Bosk;
import works.bosk.BoskDriver;
import works.bosk.Catalog;
import works.bosk.CatalogReference;
import works.bosk.Entity;
import works.bosk.Identifier;
import works.bosk.Listing;
import works.bosk.ListingEntry;
import works.bosk.ListingReference;
import works.bosk.Reference;
import works.bosk.SideTable;
import works.bosk.annotations.Polyfill;
import works.bosk.bson.BsonPlugin;
import works.bosk.drivers.BufferingDriver;
import works.bosk.drivers.state.TestEntity;
import works.bosk.drivers.state.TestValues;
import works.bosk.exceptions.FlushFailureException;
import works.bosk.exceptions.InvalidTypeException;
import works.bosk.junit.ParametersByName;
import works.bosk.junit.Slow;
import works.bosk.util.Classes;

import static ch.qos.logback.classic.Level.ERROR;
import static java.lang.Long.max;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static works.bosk.BoskTestUtils.boskName;
import static works.bosk.ListingEntry.LISTING_ENTRY;

/**
 * Tests {@link MongoDriver}-specific functionality not covered by {@link MongoDriverConformanceTest}.
 */
class MongoDriverSpecialTest extends AbstractMongoDriverTest {
	@ParametersByName
	public MongoDriverSpecialTest(TestParameters.ParameterSet parameters) {
		super(parameters.driverSettingsBuilder());
	}

	@SuppressWarnings("unused")
	static Stream<TestParameters.ParameterSet> parameters() {
		return TestParameters.driverSettings(
			Stream.of(
				MongoDriverSettings.DatabaseFormat.SEQUOIA,
//				PandoFormat.oneBigDocument(),
				PandoFormat.withGraftPoints("/catalog", "/sideTable")
			),
			Stream.of(TestParameters.EventTiming.NORMAL)
		);
	}

	@ParametersByName
	@UsesMongoService
	void warmStart_stateMatches() throws InvalidTypeException, InterruptedException, IOException {
		Bosk<TestEntity> setupBosk = new Bosk<TestEntity>(boskName("Setup"), TestEntity.class, this::initialRoot, driverFactory);
		Refs refs = setupBosk.buildReferences(Refs.class);

		// Make a change to the bosk so it's not just the initial root
		setupBosk.driver().submitReplacement(refs.listingEntry(entity123), LISTING_ENTRY);
		setupBosk.driver().flush();
		TestEntity expected = initialRoot(setupBosk)
			.withListing(Listing.of(refs.catalog(), entity123));

		Bosk<TestEntity> latecomerBosk = new Bosk<TestEntity>(boskName("Latecomer"), TestEntity.class, b->{
			throw new AssertionError("Default root function should not be called");
		}, driverFactory);

		try (var _ = latecomerBosk.readContext()) {
			TestEntity actual = latecomerBosk.rootReference().value();
			assertEquals(expected, actual);
		}
	}

	@ParametersByName
	@UsesMongoService
	void flush_localStateUpdated() throws InvalidTypeException, InterruptedException, IOException {
		// Set up MongoDriver writing to a modified BufferingDriver that lets us
		// have tight control over all the comings and goings from MongoDriver.
		BlockingQueue<Reference<?>> replacementsSeen = new LinkedBlockingDeque<>();
		Bosk<TestEntity> bosk = new Bosk<TestEntity>(boskName(), TestEntity.class, this::initialRoot,
			(b,d) -> driverFactory.build(b, new BufferingDriver(d) {
				@Override
				public <T> void submitReplacement(Reference<T> target, T newValue) {
					super.submitReplacement(target, newValue);
					replacementsSeen.add(target);
				}
			}));

		CatalogReference<TestEntity> catalogRef = bosk.rootReference().thenCatalog(TestEntity.class,
			TestEntity.Fields.catalog);
		ListingReference<TestEntity> listingRef = bosk.rootReference().thenListing(TestEntity.class,
			TestEntity.Fields.listing);

		// Make a change
		Reference<ListingEntry> ref = listingRef.then(entity123);
		bosk.driver().submitReplacement(ref, LISTING_ENTRY);

		// Give the driver a bit of time to make a mistake, if it's going to
		long budgetMillis = 2000;
		while (budgetMillis > 0) {
			long startTime = currentTimeMillis();
			Reference<?> updatedRef = replacementsSeen.poll(budgetMillis, MILLISECONDS);
			if (ref.equals(updatedRef)) {
				// We've seen the expected update. This is pretty likely to be a good time
				// to proceed with the test.
				break;
			} else {
				long elapsedTime = currentTimeMillis() - startTime;
				budgetMillis -= max(elapsedTime, 1); // Always make progress despite the vagaries of the system clock
			}
		}

		try (var _ = bosk.readContext()) {
			TestEntity expected = initialRoot(bosk);
			TestEntity actual = bosk.rootReference().value();
			assertEquals(expected, actual, "MongoDriver should not have called downstream.flush() yet");
		}

		bosk.driver().flush();

		try (var _ = bosk.readContext()) {
			TestEntity expected = initialRoot(bosk).withListing(Listing.of(catalogRef, entity123));
			TestEntity actual = bosk.rootReference().value();
			assertEquals(expected, actual, "MongoDriver.flush() should reliably update the bosk");
		}

	}

	@ParametersByName
	@UsesMongoService
	void listing_stateMatches() throws InvalidTypeException, InterruptedException, IOException {
		Bosk<TestEntity> bosk = new Bosk<TestEntity>(boskName(), TestEntity.class, this::initialRoot, driverFactory);
		BoskDriver driver = bosk.driver();
		CatalogReference<TestEntity> catalogRef = bosk.rootReference().thenCatalog(TestEntity.class,
			TestEntity.Fields.catalog);
		ListingReference<TestEntity> listingRef = bosk.rootReference().thenListing(TestEntity.class,
			TestEntity.Fields.listing);

		// Clear the listing
		driver.submitReplacement(listingRef, Listing.empty(catalogRef));

		// Add to the listing
		driver.submitReplacement(listingRef.then(entity124), LISTING_ENTRY);
		driver.submitReplacement(listingRef.then(entity123), LISTING_ENTRY);
		driver.submitReplacement(listingRef.then(entity124), LISTING_ENTRY);

		// Check the contents
		driver.flush();
		try (var _ = bosk.readContext()) {
			Listing<TestEntity> actual = listingRef.value();
			Listing<TestEntity> expected = Listing.of(catalogRef, entity124, entity123);
			assertEquals(expected, actual);
		}

		// Remove an entry
		driver.submitDeletion(listingRef.then(entity123));

		// Check the contents
		driver.flush();
		try (var _ = bosk.readContext()) {
			Listing<TestEntity> actual = listingRef.value();
			Listing<TestEntity> expected = Listing.of(catalogRef, entity124);
			assertEquals(expected, actual);
		}
	}

	@ParametersByName
	@DisruptsMongoService
	void networkOutage_boskRecovers() throws InvalidTypeException, InterruptedException, IOException {
		setLogging(ERROR, MainDriver.class, ChangeReceiver.class);

		Bosk<TestEntity> bosk = new Bosk<TestEntity>(boskName("Main"), TestEntity.class, this::initialRoot, driverFactory);
		Refs refs = bosk.buildReferences(Refs.class);
		BoskDriver driver = bosk.driver();

		LOGGER.debug("Wait till MongoDB is up and running");
		driver.flush();

		LOGGER.debug("Make another bosk that doesn't witness any change stream events before the outage");
		Bosk<TestEntity> latecomerBosk = new Bosk<TestEntity>(boskName("Latecomer"), TestEntity.class, this::initialRoot, driverFactory);

		LOGGER.debug("Cut connection");
		mongoService.proxy().setConnectionCut(true);
		tearDownActions.add(()->mongoService.proxy().setConnectionCut(false));

		assertThrows(FlushFailureException.class, driver::flush);
		assertThrows(FlushFailureException.class, latecomerBosk.driver()::flush);

		LOGGER.debug("Reestablish connection");
		mongoService.proxy().setConnectionCut(false);

		LOGGER.debug("Make a change to the bosk and verify that it gets through");
		driver.submitReplacement(refs.listingEntry(entity123), LISTING_ENTRY);
		TestEntity expected = initialRoot(bosk)
			.withListing(Listing.of(refs.catalog(), entity123));


		driver.flush();
		TestEntity actual;
		try (var _ = bosk.readContext()) {
			actual = bosk.rootReference().value();
		}
		assertEquals(expected, actual);

		latecomerBosk.driver().flush();
		TestEntity latecomerActual;
		try (var _ = latecomerBosk.readContext()) {
			latecomerActual = latecomerBosk.rootReference().value();
		}
		assertEquals(expected, latecomerActual);
	}

	@ParametersByName
	@DisruptsMongoService
	void hookRegisteredDuringNetworkOutage_works() throws InvalidTypeException, InterruptedException, IOException {
		setLogging(ERROR, MainDriver.class, ChangeReceiver.class);

		Bosk<TestEntity> bosk = new Bosk<TestEntity>(boskName(), TestEntity.class, this::initialRoot, driverFactory);
		Refs refs = bosk.buildReferences(Refs.class);
		BoskDriver driver = bosk.driver();
		CountDownLatch listingEntry124Exists = new CountDownLatch(1);

		bosk.registerHook("notice 124", refs.listingEntry(entity124), ref -> {
			if (ref.exists()) {
				listingEntry124Exists.countDown();
			}
		});

		LOGGER.debug("Wait till MongoDB is up and running");
		driver.flush();

		LOGGER.debug("Cut connection");
		mongoService.proxy().setConnectionCut(true);
		tearDownActions.add(()->mongoService.proxy().setConnectionCut(false));

		assertThrows(FlushFailureException.class, driver::flush);

		LOGGER.debug("Register hook");
		bosk.registerHook("populateListing", refs.catalog(), ref -> {
			LOGGER.debug("Hook populating listing with all ids from catalog");
			try {
				bosk.driver().submitReplacement(refs.listing(), Listing.of(refs.catalog(), ref.value().ids()));
			} catch (DisconnectedException e) {
				LOGGER.debug("Driver is disconnected. We're expecting this to happen at least once.", e);
			}
		});

		LOGGER.debug("Reestablish connection");
		mongoService.proxy().setConnectionCut(false);

		LOGGER.debug("Ensure populateListing hook has been triggered");
		driver.flush();

		LOGGER.debug("Wait for listing entry 124 to exist");
		boolean success = listingEntry124Exists.await(30, SECONDS);
		assertTrue(success, "Entry 124 wait should not time out");

		LOGGER.debug("Check bosk state");
		TestEntity expected = initialRoot(bosk)
			.withListing(Listing.of(refs.catalog(), entity123, entity124));

		TestEntity actual;
		try (var _ = bosk.readContext()) {
			actual = bosk.rootReference().value();
		}
		assertEquals(expected, actual);
	}

	@ParametersByName
	@UsesMongoService
	void initialStateHasNonexistentFields_ignored() throws InvalidTypeException {
		setLogging(ERROR, BsonPlugin.class);

		// Upon creating bosk, the initial value will be saved to MongoDB
		new Bosk<TestEntity>(boskName("Newer"), TestEntity.class, this::initialRootWithValues, driverFactory);

		// Upon creating prevBosk, the state in the database will be loaded into the local.
		Bosk<OldEntity> prevBosk = new Bosk<OldEntity>(
			boskName("Prev"),
			OldEntity.class,
			(b) -> { throw new AssertionError("prevBosk should use the state from MongoDB"); },
			createDriverFactory(logController));

		OldEntity expected = OldEntity.withString(rootID.toString(), prevBosk);

		OldEntity actual;
		try (var _ = prevBosk.readContext()) {
			actual = prevBosk.rootReference().value();
		}
		assertEquals(expected, actual);
	}

	@ParametersByName
	@UsesMongoService
	void updateHasNonexistentFields_ignored() throws InvalidTypeException, IOException, InterruptedException {
		setLogging(ERROR, BsonPlugin.class);

		Bosk<TestEntity> bosk = new Bosk<TestEntity>(boskName("Newer"), TestEntity.class, this::initialRootWithEmptyCatalog, driverFactory);
		Bosk<OldEntity> prevBosk = new Bosk<OldEntity>(
			boskName("Prev"),
			OldEntity.class,
			(b) -> { throw new AssertionError("prevBosk should use the state from MongoDB"); },
			createDriverFactory(logController));

		TestEntity initialRoot = initialRootWithEmptyCatalog(bosk);
		bosk.driver().submitReplacement(bosk.rootReference(),
			initialRoot
				.withString("replacementString")
				.withValues(Optional.of(TestValues.blank())));

		prevBosk.driver().flush();

		OldEntity oldEntity = OldEntity.withString("replacementString", prevBosk);

		OldEntity actual;
		try (var _ = prevBosk.readContext()) {
			actual = prevBosk.rootReference().value();
		}

		assertEquals(oldEntity, actual);
	}

	@ParametersByName
	@UsesMongoService
	void updateNonexistentField_ignored() throws InvalidTypeException, IOException, InterruptedException {
		setLogging(ERROR, SequoiaFormatDriver.class, PandoFormatDriver.class, BsonPlugin.class);

		Bosk<TestEntity> bosk = new Bosk<TestEntity>(boskName("Newer"), TestEntity.class, this::initialRootWithEmptyCatalog, driverFactory);
		Bosk<OldEntity> prevBosk = new Bosk<OldEntity>(
			boskName("Prev"),
			OldEntity.class,
			(b) -> { throw new AssertionError("prevBosk should use the state from MongoDB"); },
			createDriverFactory(logController));

		Refs refs = bosk.buildReferences(Refs.class);
		bosk.driver().submitReplacement(refs.values(),
			TestValues.blank());

		prevBosk.driver().flush();

		OldEntity expected = OldEntity // unchanged from before
			.withString(rootID.toString(), prevBosk);

		OldEntity actual;
		try (var _ = prevBosk.readContext()) {
			actual = prevBosk.rootReference().value();
		}

		assertEquals(expected, actual);
	}

	@ParametersByName
	@UsesMongoService
	void updateInsidePolyfill_works() throws IOException, InterruptedException, InvalidTypeException {
		// We'll use this as an honest observer of the actual state
		LOGGER.debug("Create Original bosk");
		Bosk<TestEntity> originalBosk = new Bosk<TestEntity>(
			boskName("Original"),
			TestEntity.class,
			this::initialRoot,
			createDriverFactory(logController)
		);

		LOGGER.debug("Create Upgradeable bosk");
		Bosk<UpgradeableEntity> upgradeableBosk = new Bosk<UpgradeableEntity>(
			boskName("Upgradeable"),
			UpgradeableEntity.class,
			(b) -> { throw new AssertionError("upgradeableBosk should use the state from MongoDB"); },
			createDriverFactory(logController)
		);

		LOGGER.debug("Ensure polyfill returns the right value on read");
		TestValues polyfill;
		try (var _ = upgradeableBosk.readContext()) {
			polyfill = upgradeableBosk.rootReference().value().values();
		}
		assertEquals(TestValues.blank(), polyfill);

		LOGGER.debug("Check state before");
		Optional<TestValues> before;
		try (var _ = originalBosk.readContext()) {
			before = originalBosk.rootReference().value().values();
		}
		assertEquals(Optional.empty(), before); // Not there yet

		LOGGER.debug("Perform update inside polyfill");
		Refs refs = upgradeableBosk.buildReferences(Refs.class);
		upgradeableBosk.driver().submitReplacement(refs.valuesString(), "new value");
		originalBosk.driver().flush(); // Not the bosk that did the update!

		LOGGER.debug("Check state after");
		String after;
		try (var _ = originalBosk.readContext()) {
			after = originalBosk.rootReference().value().values().get().string();
		}
		assertEquals("new value", after); // Now it's there
	}

	@ParametersByName
	@UsesMongoService
	void deleteNonexistentField_ignored() throws InvalidTypeException, IOException, InterruptedException {
		setLogging(ERROR, SequoiaFormatDriver.class, PandoFormatDriver.class);

		Bosk<TestEntity> newerBosk = new Bosk<TestEntity>(boskName("Newer"), TestEntity.class, this::initialRootWithEmptyCatalog, driverFactory);
		Bosk<OldEntity> prevBosk = new Bosk<OldEntity>(
			boskName("Prev"),
			OldEntity.class,
			(b) -> { throw new AssertionError("prevBosk should use the state from MongoDB"); },
			createDriverFactory(logController));

		Refs refs = newerBosk.buildReferences(Refs.class);
		newerBosk.driver().submitDeletion(refs.values());

		prevBosk.driver().flush();

		OldEntity oldEntity = OldEntity.withString(rootID.toString(), prevBosk); // unchanged

		OldEntity actual;
		try (var _ = prevBosk.readContext()) {
			actual = prevBosk.rootReference().value();
		}

		assertEquals(oldEntity, actual);
	}

	@ParametersByName
	@UsesMongoService
	@Slow
	void databaseMissingField_fallsBackToDefaultState() throws InvalidTypeException, IOException, InterruptedException {
		setLogging(ERROR, ChangeReceiver.class);

		LOGGER.debug("Set up database with entity that has no string field");
		Bosk<OptionalEntity> setupBosk = new Bosk<OptionalEntity>(boskName("Setup"), OptionalEntity.class, b -> OptionalEntity.withString(Optional.empty(), b), createDriverFactory(logController));

		LOGGER.debug("Connect another bosk where the string field is mandatory");
		Bosk<TestEntity> testBosk = new Bosk<TestEntity>(boskName("Test"), TestEntity.class, this::initialRoot, driverFactory);
		TestEntity expected1 = initialRoot(testBosk); // NOT what was put there by the setup bosk!
		TestEntity actual1;
		try (var _ = testBosk.readContext()) {
			actual1 = testBosk.rootReference().value();
		}

		assertEquals(expected1, actual1, "Disconnected bosk should use the default initial root");

		LOGGER.debug("Repair the bosk by writing the string value");
		setupBosk.driver().submitReplacement(
			setupBosk.rootReference().then(String.class, "string"),
			"stringValue");

		LOGGER.debug("Flush testBosk to get the state from the database");
		testBosk.driver().flush();

		Refs refs = testBosk.buildReferences(Refs.class);
		TestEntity expected2;
		try (var _ = setupBosk.readContext()) {
			// (Note that we don't bother flushing setupBosk because we don't need the latest value;
			// the variant field hasn't changed since it was initialized.)
			expected2 = TestEntity.empty(Identifier.from("optionalEntity"), refs.catalog())
				.withString("stringValue")
				.withVariant(setupBosk.rootReference().value().variant());
		}

		TestEntity actual2;
		try (var _ = testBosk.readContext()) {
			actual2 = testBosk.rootReference().value();
		}

		assertEquals(expected2, actual2, "Reconnected bosk should see the state from the database");
	}

	@ParametersByName
	@UsesMongoService
	void unrelatedDatabase_ignored() throws InvalidTypeException, IOException, InterruptedException {
		tearDownActions.addFirst(mongoService.client().getDatabase("unrelated")::drop);
		doUnrelatedChangeTest("unrelated", MainDriver.COLLECTION_NAME, rootDocumentID().getValue());
	}

	@ParametersByName
	@UsesMongoService
	void unrelatedCollection_ignored() throws InvalidTypeException, IOException, InterruptedException {
		doUnrelatedChangeTest(driverSettings.database(), "unrelated", rootDocumentID().getValue());
	}

	@ParametersByName
	@UsesMongoService
	void unrelatedDoc_ignored() throws InvalidTypeException, IOException, InterruptedException {
		doUnrelatedChangeTest(driverSettings.database(), MainDriver.COLLECTION_NAME, "unrelated");
	}

	private void doUnrelatedChangeTest(String databaseName, String collectionName, String docID) throws IOException, InterruptedException, InvalidTypeException {
		Bosk<TestEntity> bosk = new Bosk<TestEntity>(boskName(), TestEntity.class, this::initialRoot, driverFactory);

		MongoCollection<Document> counterfeitCollection = mongoService.client()
			.getDatabase(databaseName)
			.getCollection(collectionName);

		// Make a realistic-looking doc to try to fool the driver
		MongoCollection<Document> actualCollection = mongoService.client()
			.getDatabase(driverSettings.database())
			.getCollection(MainDriver.COLLECTION_NAME);
		Document doc;
		try (MongoCursor<Document> cursor = actualCollection.find().limit(1).cursor()) {
			doc = cursor.next();
		}
		doc.put("_id", docID);
		doc.get("state", Document.class).put("string", "counterfeit");
		counterfeitCollection.insertOne(doc);

		bosk.driver().flush();
		TestEntity expected = initialRoot(bosk);
		try (var _ = bosk.readContext()) {
			TestEntity actual = bosk.rootReference().value();
			assertEquals(expected, actual);
		}
	}

	@ParametersByName
	@UsesMongoService
	void refurbish_createsField() throws IOException, InterruptedException {
		// We'll use this as an honest observer of the actual state
		LOGGER.debug("Create Original bosk");
		Bosk<TestEntity> originalBosk = new Bosk<TestEntity>(
			boskName("Original"),
			TestEntity.class,
			this::initialRoot,
			createDriverFactory(logController)
		);

		LOGGER.debug("Create Upgradeable bosk");
		Bosk<UpgradeableEntity> upgradeableBosk = new Bosk<UpgradeableEntity>(
			boskName("Upgradeable"),
			UpgradeableEntity.class,
			(b) -> { throw new AssertionError("upgradeableBosk should use the state from MongoDB"); },
			createDriverFactory(logController)
		);

		LOGGER.debug("Check state before");
		Optional<TestValues> before;
		try (var _ = originalBosk.readContext()) {
			before = originalBosk.rootReference().value().values();
		}
		assertEquals(Optional.empty(), before); // Not there yet

		LOGGER.debug("Call refurbish");
		upgradeableBosk.getDriver(MongoDriver.class).refurbish();
		originalBosk.driver().flush(); // Not the bosk that did refurbish!

		LOGGER.debug("Check state after");
		Optional<TestValues> after;
		try (var _ = originalBosk.readContext()) {
			after = originalBosk.rootReference().value().values();
		}
		assertEquals(Optional.of(TestValues.blank()), after); // Now it's there
	}

	@ParametersByName
	@UsesMongoService
	@Slow
	void manifestVersionBump_disconnects() throws IOException, InterruptedException {
		setLogging(ERROR, MainDriver.class, ChangeReceiver.class);

		Bosk<TestEntity> bosk = new Bosk<TestEntity>(
			boskName(),
			TestEntity.class,
			this::initialRoot,
			createDriverFactory(logController)
		);

		LOGGER.debug("Flush should work");
		bosk.driver().flush();

		LOGGER.debug("Upgrade to an unsupported manifest version");
		MongoCollection<Document> collection = mongoService.client()
			.getDatabase(driverSettings.database())
			.getCollection(MainDriver.COLLECTION_NAME);
		collection.updateOne(
			new BsonDocument("_id", new BsonString("manifest")),
			new BsonDocument("$inc", new BsonDocument("version", new BsonInt32(1)))
		);
		// Must also bump the revision number or else flush rightly does nothing
		collection.updateOne(
			new BsonDocument("_id", rootDocumentID()),
			new BsonDocument("$inc", new BsonDocument("revision", new BsonInt64(1)))
		);

		LOGGER.debug("Flush should throw");
		assertThrows(FlushFailureException.class, ()->bosk.driver().flush());

		LOGGER.debug("Finished");
	}

	@ParametersByName
	@UsesMongoService
	void refurbish_fixesMetadata() throws IOException, InterruptedException {
		// Set up the database so it looks basically right
		Bosk<TestEntity> initialBosk = new Bosk<TestEntity>(
			boskName("Initial"),
			TestEntity.class,
			this::initialRoot,
			createDriverFactory(logController)
		);

		// (Close this so it doesn't crash when we delete the "path" field)
		initialBosk.getDriver(MongoDriver.class).close();

		// Delete some metadata fields
		MongoCollection<Document> collection = mongoService.client()
			.getDatabase(driverSettings.database())
			.getCollection(MainDriver.COLLECTION_NAME);
		deleteFields(collection, Formatter.DocumentFields.path, Formatter.DocumentFields.revision);

		// Make the bosk whose refurbish operation we want to test
		Bosk<TestEntity> bosk = new Bosk<TestEntity>(
			boskName("Main"),
			TestEntity.class,
			this::initialRoot,
			createDriverFactory(logController)
		);

		// Get the new bosk reconnected
		bosk.driver().flush();

		// Simply connecting a new bosk repairs certain fields.
		// To test those, delete them again.
		// This may cause the receiver to throw an exception for deleting this field unexpectedly,
		// but it recovers, so that's ok.
		deleteFields(collection, Formatter.DocumentFields.revision);

		// Verify that the fields are indeed gone
		BsonDocument filterDoc = new BsonDocument("_id", rootDocumentID());
		try (MongoCursor<Document> cursor = collection.find(filterDoc).cursor()) {
			Document doc = cursor.next();
			assertNull(doc.get(Formatter.DocumentFields.path.name()));
			assertNull(doc.get(Formatter.DocumentFields.revision.name()));
		}

		// Refurbish
		bosk.getDriver(MongoDriver.class).refurbish();

		// Verify the fields are all now there
		try (MongoCursor<Document> cursor = collection.find(filterDoc).cursor()) {
			Document doc = cursor.next();
			assertEquals("/", doc.get(Formatter.DocumentFields.path.name()));
			assertEquals(1L, doc.getLong(Formatter.DocumentFields.revision.name()));
		}

	}

	private void deleteFields(MongoCollection<Document> collection, Formatter.DocumentFields... fields) {
		BsonDocument fieldsToUnset = new BsonDocument();
		for (Formatter.DocumentFields field: fields) {
			fieldsToUnset.append(field.name(), new BsonNull()); // Value is ignored
		}
		BsonDocument filterDoc = new BsonDocument("_id", rootDocumentID());
		collection.updateOne(
			filterDoc,
			new BsonDocument("$unset", fieldsToUnset));

		// Let's just make sure they're gone
		try (MongoCursor<Document> cursor = collection.find(filterDoc).cursor()) {
			Document doc = cursor.next();
			for (Formatter.DocumentFields field: fields) {
				assertNull(doc.get(field.name()));
			}
		}
	}

	@NotNull
	private BsonString rootDocumentID() {
		return (MongoDriverSettings.DatabaseFormat.SEQUOIA == driverSettings.preferredDatabaseFormat())
			? SequoiaFormatDriver.DOCUMENT_ID
			: PandoFormatDriver.ROOT_DOCUMENT_ID;
	}

	/**
	 * Represents an earlier version of the entity before some fields were added.
	 */
	@With
	public record OldEntity(
		Identifier id,
		String string,
		// We need catalog and sideTable because we use them in our PandoConfiguration
		Catalog<OldEntity> catalog,
		SideTable<OldEntity, OldEntity> sideTable
	) implements Entity {
		public static OldEntity withString(String value, Bosk<OldEntity> bosk) throws InvalidTypeException {
			Reference<Catalog<OldEntity>> catalogRef = bosk.rootReference().then(Classes.catalog(OldEntity.class), "catalog");
			return new OldEntity(
				rootID,
				value,
				Catalog.empty(),
				SideTable.empty(catalogRef)
			);
		}
	}

	/**
	 * A version of {@link TestEntity} where the {@link Optional} {@link TestEntity#values()}
	 * field has a polyfill.
	 */
	public record UpgradeableEntity(
		Identifier id,
		String string,
		Catalog<TestEntity> catalog,
		Listing<TestEntity> listing,
		SideTable<TestEntity, TestEntity> sideTable,
		TestEntity.Variant variant,
		TestValues values
	) implements Entity {
		@Polyfill("values")
		static final TestValues DEFAULT_VALUES = works.bosk.drivers.state.TestValues.blank();
	}

	/**
	 * A version of {@link TestEntity} where all the fields are {@link Optional} so we
	 * have full control over what fields we set.
	 */
	@With
	public record OptionalEntity(
		Identifier id,
		Optional<String> string,
		Optional<Catalog<TestEntity>> catalog,
		Optional<Listing<TestEntity>> listing,
		Optional<SideTable<TestEntity, TestEntity>> sideTable,
		TestEntity.Variant variant,
		Optional<TestValues> values
	) implements Entity {
		static OptionalEntity withString(Optional<String> string, Bosk<OptionalEntity> bosk) throws InvalidTypeException {
			CatalogReference<TestEntity> domain = bosk.rootReference().thenCatalog(TestEntity.class, "catalog");
			return new OptionalEntity(
				Identifier.from("optionalEntity"),
				string,
				Optional.of(Catalog.empty()),
				Optional.of(Listing.empty(domain)),
				Optional.of(SideTable.empty(domain)),
				new TestEntity.StringCase("stringCase"),
				java.util.Optional.empty());
		}
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(MongoDriverSpecialTest.class);
}
