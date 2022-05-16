package org.vena.bosk.drivers.dynamo;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.Shard;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.amazonaws.services.dynamodbv2.model.Stream;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.mongodb.MongoException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.BiFunction;
import lombok.Value;
import lombok.experimental.Accessors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.vena.bosk.Bosk;
import org.vena.bosk.BoskDriver;
import org.vena.bosk.Catalog;
import org.vena.bosk.CatalogReference;
import org.vena.bosk.Entity;
import org.vena.bosk.Identifier;
import org.vena.bosk.Listing;
import org.vena.bosk.ListingEntry;
import org.vena.bosk.ListingReference;
import org.vena.bosk.Path;
import org.vena.bosk.Reference;
import org.vena.bosk.SideTable;
import org.vena.bosk.drivers.BufferingDriver;
import org.vena.bosk.drivers.DriverConformanceTest;
import org.vena.bosk.exceptions.InvalidTypeException;

import static com.amazonaws.services.dynamodbv2.model.StreamViewType.NEW_IMAGE;
import static java.lang.Boolean.TRUE;
import static java.lang.Long.max;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.vena.bosk.ListingEntry.LISTING_ENTRY;

@Testcontainers
@Disabled("Proof of concept. Work in progress. Under construction.")
class DynamoDriverTest extends DriverConformanceTest {
	public static final String TEST_DB = "testDB";
	public static final String TEST_COLLECTION = "testCollection";
	protected static final Identifier entity123 = Identifier.from("123");
	protected static final Identifier entity124 = Identifier.from("124");
	protected static final Identifier rootID = Identifier.from("root");

	private final Deque<Runnable> tearDownActions = new ArrayDeque<>();

	private static final Network NETWORK = Network.newNetwork();

	@Container
	private static final GenericContainer<?> DYNAMO_CONTAINER = DynamoContainerHelpers.dynamoContainer(NETWORK);

	@Container
	private static final ToxiproxyContainer TOXIPROXY_CONTAINER = DynamoContainerHelpers.toxiproxyContainer(NETWORK);

	private static ToxiproxyContainer.ContainerProxy proxy;

	private static DynamoDriverSettings driverSettings;

	@BeforeAll
	static void setupDatabase() {
		proxy = TOXIPROXY_CONTAINER.getProxy(DYNAMO_CONTAINER, 8000);
		driverSettings = DynamoDriverSettings.builder()
			.build();
	}

	@AfterAll
	static void deleteDatabase() {
//		MongoClient mongoClient = MongoClients.create(clientSettings);
//		mongoClient.getDatabase(TEST_DB).drop();
//		mongoClient.close();
	}

	@BeforeEach
	void setupDriverFactory() {
		driverFactory = createDriverFactory();
	}

	@AfterEach
	void runTearDown() {
//		MongoClient mongoClient = MongoClients.create(clientSettings);
//		tearDownActions.forEach(a -> a.accept(mongoClient));
//		mongoClient.close();
	}

	@Test
	void testDynamo() throws InterruptedException {
		AmazonDynamoDBStreams streamClient = AmazonDynamoDBStreamsClientBuilder
			.standard()
			.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:" + proxy.getProxyPort(), "ca-central-1"))
			.withCredentials(new AWSStaticCredentialsProvider(
				new BasicAWSCredentials("dummy-key", "dummy-secret")))
			.build();

		AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder
			.standard()
			.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:" + proxy.getProxyPort(), "ca-central-1"))
			.withCredentials(new AWSStaticCredentialsProvider(
				new BasicAWSCredentials("dummy-key", "dummy-secret")))
			.build();

		String streamArn;

		String tableName = "TestTable";
		{
			CreateTableRequest request = new CreateTableRequest()
				.withTableName(tableName)
				.withAttributeDefinitions(new AttributeDefinition(
					"id", ScalarAttributeType.S))
				.withKeySchema(new KeySchemaElement("id", KeyType.HASH))
				.withProvisionedThroughput(new ProvisionedThroughput(
					10L, 10L)) // TODO: Test throttling scenarios
				.withStreamSpecification(new StreamSpecification()
					.withStreamEnabled(TRUE)
					.withStreamViewType(NEW_IMAGE))
				;
			CreateTableResult result = ddb.createTable(request);
			LOGGER.info("Created table: {}", result);
			streamArn = result.getTableDescription().getLatestStreamArn();
		}

		{
			UpdateItemRequest request = new UpdateItemRequest()
				.withTableName(tableName)
				.addKeyEntry("id", new AttributeValue("MyItem"))
				.addAttributeUpdatesEntry("hello", new AttributeValueUpdate(new AttributeValue("world"), AttributeAction.PUT))
				;
			UpdateItemResult response = ddb.updateItem(request);
			LOGGER.info("Updated item: {}", response);
		}

		{
			GetItemRequest request = new GetItemRequest()
				.withTableName(tableName)
				.addKeyEntry("id", new AttributeValue("MyItem"))
				;
			GetItemResult response = ddb.getItem(request);
			LOGGER.info("Queried item: {}", response);
		}

		{
			// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.LowLevel.Walkthrough.html

			// Get all the shard IDs from the stream.  Note that DescribeStream returns
			// the shard IDs one page at a time.
			String lastEvaluatedShardId = null;

			do {
				DescribeStreamResult describeStreamResult = streamClient.describeStream(
					new DescribeStreamRequest()
						.withStreamArn(streamArn)
						.withExclusiveStartShardId(lastEvaluatedShardId));
				List<Shard> shards = describeStreamResult.getStreamDescription().getShards();

				// Process each shard on this page

				for (Shard shard : shards) {
					LOGGER.info("Shard: {}", shard);
					String shardId = shard.getShardId();

					// Get an iterator for the current shard

					GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest()
						.withStreamArn(streamArn)
						.withShardId(shardId)
						.withShardIteratorType(ShardIteratorType.TRIM_HORIZON);
					GetShardIteratorResult getShardIteratorResult =
						streamClient.getShardIterator(getShardIteratorRequest);
					String currentShardIter = getShardIteratorResult.getShardIterator();

					// Shard iterator is not null until the Shard is sealed (marked as READ_ONLY).
					// To prevent running the loop until the Shard is sealed, which will be on average
					// 4 hours, we process only the items that were written into DynamoDB and then exit.
					int processedRecordCount = 0;
					while (currentShardIter != null) {
						LOGGER.info("    Shard iterator: {}", currentShardIter);

						// Use the shard iterator to read the stream records

						GetRecordsResult getRecordsResult = streamClient.getRecords(new GetRecordsRequest()
							.withShardIterator(currentShardIter));
						LOGGER.info("    GetRecordsResult: {}", getRecordsResult);
						List<Record> records = getRecordsResult.getRecords();
						if (records.size() == 0) {
							LOGGER.info("    No more records. This is where we ought to poll, but for now, let's just quit.");
							break;
						} else {
							for (Record record : records) {
								LOGGER.info("        Record: {}", record);
								LOGGER.info("          -> {}", record.getDynamodb().getNewImage());
							}
							processedRecordCount += records.size();
						}
						currentShardIter = getRecordsResult.getNextShardIterator();
					}
				}

				// If LastEvaluatedShardId is set, then there is
				// at least one more page of shard IDs to retrieve
				lastEvaluatedShardId = describeStreamResult.getStreamDescription().getLastEvaluatedShardId();

			} while (lastEvaluatedShardId != null);

		}

		if (false)
		{
			ListStreamsResult listStreamsResult = streamClient.listStreams(new ListStreamsRequest()
				.withTableName(tableName));
			LOGGER.info("ListStreamsResult: {}", listStreamsResult);
			for (Stream stream: listStreamsResult.getStreams()) {
				DescribeStreamResult describeStreamResult = streamClient
					.describeStream(new DescribeStreamRequest()
						.withStreamArn(stream.getStreamArn()));
				LOGGER.info("DescribeStreamResult: {}", describeStreamResult);
				for (Shard shard: describeStreamResult.getStreamDescription().getShards()) {
					GetRecordsResult getRecordsResult = streamClient.getRecords(new GetRecordsRequest().withShardIterator(shard.getShardId()));
					LOGGER.info("GetRecordsResult: {}", getRecordsResult);
					for (Record record: getRecordsResult.getRecords()) {
						LOGGER.info("Streamed record: {}", record);
					}
				}
			}
		}
	}

	@Test
	void warmStart_stateMatches() throws InvalidTypeException, InterruptedException, IOException {
		Bosk<TestEntity> setupBosk = new Bosk<TestEntity>("Test bosk", TestEntity.class, this::initialRoot, driverFactory);
		CatalogReference<TestEntity> catalogRef = setupBosk.catalogReference(TestEntity.class, Path.just(TestEntity.Fields.catalog));
		ListingReference<TestEntity> listingRef = setupBosk.listingReference(TestEntity.class, Path.just(TestEntity.Fields.listing));

		// Make a change to the bosk so it's not just the initial root
		setupBosk.driver().submitReplacement(listingRef.then(entity123), LISTING_ENTRY);
		setupBosk.driver().flush();
		TestEntity expected = initialRoot(setupBosk)
			.withListing(Listing.of(catalogRef, entity123));

		Bosk<TestEntity> latecomerBosk = new Bosk<TestEntity>("Latecomer bosk", TestEntity.class, b->{
			throw new AssertionError("Default root function should not be called");
		}, driverFactory);

		try (@SuppressWarnings("unused") Bosk<TestEntity>.ReadContext context = latecomerBosk.readContext()) {
			TestEntity actual = latecomerBosk.rootReference().value();
			assertEquals(expected, actual);
		}

	}

	@Test
	void flush_localStateUpdated() throws InvalidTypeException, InterruptedException, IOException {
		// Set up MongoDriver writing to a modified BufferingDriver that lets us
		// have tight control over all the comings and goings from MongoDriver.
		BlockingQueue<Reference<?>> replacementsSeen = new LinkedBlockingDeque<>();
		Bosk<TestEntity> bosk = new Bosk<TestEntity>("Test bosk", TestEntity.class, this::initialRoot,
			(d,b) -> driverFactory.apply(new BufferingDriver<TestEntity>(d){
				@Override
				public <T> void submitReplacement(Reference<T> target, T newValue) {
					super.submitReplacement(target, newValue);
					replacementsSeen.add(target);
				}
			}, b));

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

		try (@SuppressWarnings("unused") Bosk<TestEntity>.ReadContext context = bosk.readContext()) {
			TestEntity expected = initialRoot(bosk);
			TestEntity actual = bosk.rootReference().value();
			assertEquals(expected, actual, "MongoDriver should not have called downstream.flush() yet");
		}

		bosk.driver().flush();

		try (@SuppressWarnings("unused") Bosk<TestEntity>.ReadContext context = bosk.readContext()) {
			TestEntity expected = initialRoot(bosk).withListing(Listing.of(catalogRef, entity123));
			TestEntity actual = bosk.rootReference().value();
			assertEquals(expected, actual, "MongoDriver.flush() should reliably update the bosk");
		}

	}

	@Test
	void listing_stateMatches() throws InvalidTypeException, InterruptedException, IOException {
		Bosk<TestEntity> bosk = new Bosk<TestEntity>("Test bosk", TestEntity.class, this::initialRoot, driverFactory);
		BoskDriver<TestEntity> driver = bosk.driver();
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
		try (@SuppressWarnings("unused") Bosk<?>.ReadContext readContext = bosk.readContext()) {
			Listing<TestEntity> actual = listingRef.value();
			Listing<TestEntity> expected = Listing.of(catalogRef, entity124, entity123);
			assertEquals(expected, actual);
		}

		// Remove an entry
		driver.submitDeletion(listingRef.then(entity123));

		// Check the contents
		driver.flush();
		try (@SuppressWarnings("unused") Bosk<?>.ReadContext readContext = bosk.readContext()) {
			Listing<TestEntity> actual = listingRef.value();
			Listing<TestEntity> expected = Listing.of(catalogRef, entity124);
			assertEquals(expected, actual);
		}
	}

	@Test
	void networkOutage_boskRecovers() throws InvalidTypeException, InterruptedException, IOException {
		Bosk<TestEntity> bosk = new Bosk<TestEntity>("Test bosk", TestEntity.class, this::initialRoot, driverFactory);
		BoskDriver<TestEntity> driver = bosk.driver();
		CatalogReference<TestEntity> catalogRef = bosk.catalogReference(TestEntity.class, Path.just(TestEntity.Fields.catalog));
		ListingReference<TestEntity> listingRef = bosk.listingReference(TestEntity.class, Path.just(TestEntity.Fields.listing));

		// Wait till MongoDB is up and running
		driver.flush();

		// Make another bosk that doesn't witness any change stream events before the outage
		Bosk<TestEntity> latecomerBosk = new Bosk<TestEntity>("Latecomer bosk", TestEntity.class, this::initialRoot, driverFactory);

		proxy.setConnectionCut(true);

		assertThrows(MongoException.class, driver::flush);
		assertThrows(MongoException.class, latecomerBosk.driver()::flush);

		proxy.setConnectionCut(false);

		// Make a change to the bosk and verify that it gets through
		driver.submitReplacement(listingRef.then(entity123), LISTING_ENTRY);
		TestEntity expected = initialRoot(bosk)
			.withListing(Listing.of(catalogRef, entity123));


		driver.flush();
		TestEntity actual;
		try (@SuppressWarnings("unused") Bosk<?>.ReadContext readContext = bosk.readContext()) {
			actual = bosk.rootReference().value();
		}
		assertEquals(expected, actual);

		latecomerBosk.driver().flush();
		TestEntity latecomerActual;
		try (@SuppressWarnings("unused") Bosk<?>.ReadContext readContext = latecomerBosk.readContext()) {
			latecomerActual = latecomerBosk.rootReference().value();
		}
		assertEquals(expected, latecomerActual);
	}

	@Test
	void initialStateHasNonexistentFields_ignored() {
		// Upon creating bosk, the initial value will be saved to MongoDB
		bosk = new Bosk<TestEntity>("Newer bosk", TestEntity.class, this::initialRoot, driverFactory);

		// Upon creating prevBosk, the state in the database will be loaded into the local.
		Bosk<OldEntity> prevBosk = new Bosk<OldEntity>(
			"Older bosk",
			OldEntity.class,
			(bosk) -> { throw new AssertionError("prevBosk should use the state from MongoDB"); },
			createDriverFactory());

		OldEntity expected = new OldEntity(rootID, rootID.toString());

		OldEntity actual;
		try (@SuppressWarnings("unused") Bosk<?>.ReadContext readContext = prevBosk.readContext()) {
			actual = prevBosk.rootReference().value();
		}
		assertEquals(expected, actual);
	}

	@Test
	void updateHasNonexistentFields_ignored() throws InvalidTypeException, IOException, InterruptedException {
		bosk = new Bosk<TestEntity>("Newer bosk", TestEntity.class, this::initialRoot, driverFactory);
		Bosk<OldEntity> prevBosk = new Bosk<OldEntity>(
			"Older bosk",
			OldEntity.class,
			(bosk) -> { throw new AssertionError("prevBosk should use the state from MongoDB"); },
			createDriverFactory());

		TestEntity initialRoot = initialRoot(bosk);
		bosk.driver().submitReplacement(bosk.rootReference(),
			initialRoot
				.withString("replacementString")
				.withListing(Listing.of(initialRoot.listing().domain(), Identifier.from("newEntry"))));

		prevBosk.driver().flush();

		OldEntity expected = new OldEntity(rootID, "replacementString");

		OldEntity actual;
		try (@SuppressWarnings("unused") Bosk<?>.ReadContext readContext = prevBosk.readContext()) {
			actual = prevBosk.rootReference().value();
		}

		assertEquals(expected, actual);
	}

	@Test
	void updateNonexistentField_ignored() throws InvalidTypeException, IOException, InterruptedException {
		bosk = new Bosk<TestEntity>("Newer bosk", TestEntity.class, this::initialRoot, driverFactory);
		Bosk<OldEntity> prevBosk = new Bosk<OldEntity>(
			"Older bosk",
			OldEntity.class,
			(bosk) -> { throw new AssertionError("prevBosk should use the state from MongoDB"); },
			createDriverFactory());

		ListingReference<TestEntity> listingRef = bosk.rootReference().thenListing(TestEntity.class, TestEntity.Fields.listing);

		TestEntity initialRoot = initialRoot(bosk);
		bosk.driver().submitReplacement(listingRef,
			Listing.of(initialRoot.listing().domain(), Identifier.from("newEntry")));

		prevBosk.driver().flush();

		OldEntity expected = new OldEntity(rootID, rootID.toString()); // unchanged

		OldEntity actual;
		try (@SuppressWarnings("unused") Bosk<?>.ReadContext readContext = prevBosk.readContext()) {
			actual = prevBosk.rootReference().value();
		}

		assertEquals(expected, actual);
	}

	@Test
	void deleteNonexistentField_ignored() throws InvalidTypeException, IOException, InterruptedException {
		bosk = new Bosk<TestEntity>("Newer bosk", TestEntity.class, this::initialRoot, driverFactory);
		Bosk<OldEntity> prevBosk = new Bosk<OldEntity>(
			"Older bosk",
			OldEntity.class,
			(bosk) -> { throw new AssertionError("prevBosk should use the state from MongoDB"); },
			createDriverFactory());

		ListingReference<TestEntity> listingRef = bosk.rootReference().thenListing(TestEntity.class, TestEntity.Fields.listing);

		bosk.driver().submitDeletion(listingRef.then(entity123));

		prevBosk.driver().flush();

		OldEntity expected = new OldEntity(rootID, rootID.toString()); // unchanged

		OldEntity actual;
		try (@SuppressWarnings("unused") Bosk<?>.ReadContext readContext = prevBosk.readContext()) {
			actual = prevBosk.rootReference().value();
		}

		assertEquals(expected, actual);
	}

	private <E extends Entity> BiFunction<BoskDriver<E>, Bosk<E>, BoskDriver<E>> createDriverFactory() {
		return (downstream, bosk) -> {
			DynamoDriver<E> driver = new DynamoDriver<E>(
				downstream,
				driverSettings);
			tearDownActions.addFirst(driver::close);
			return driver;
		};
	}

	@Value
	@Accessors(fluent = true)
	public static class OldEntity implements Entity {
		Identifier id;
		String string;
	}

	private TestEntity initialRoot(Bosk<TestEntity> testEntityBosk) throws InvalidTypeException {
		Reference<Catalog<TestEntity>> catalogRef = testEntityBosk.catalogReference(TestEntity.class, Path.just(
				TestEntity.Fields.catalog
		));
		Reference<Catalog<TestEntity>> anyChildCatalog = testEntityBosk.catalogReference(TestEntity.class, Path.of(
			TestEntity.Fields.catalog, "-child-", TestEntity.Fields.catalog
		));
		return new TestEntity(rootID,
			rootID.toString(),
			Catalog.of(
				TestEntity.empty(entity123, anyChildCatalog.boundTo(entity123)),
				TestEntity.empty(entity124, anyChildCatalog.boundTo(entity124))
			),
			Listing.of(catalogRef, entity123),
			SideTable.empty(catalogRef),
			Optional.empty()
		);
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDriverTest.class);
}
