package io.vena.bosk.drivers.mongo;

import ch.qos.logback.classic.Level;
import com.mongodb.MongoClientSettings;
import io.vena.bosk.Bosk;
import io.vena.bosk.Catalog;
import io.vena.bosk.CatalogReference;
import io.vena.bosk.DriverFactory;
import io.vena.bosk.Entity;
import io.vena.bosk.Identifier;
import io.vena.bosk.Listing;
import io.vena.bosk.ListingEntry;
import io.vena.bosk.ListingReference;
import io.vena.bosk.Reference;
import io.vena.bosk.SideTable;
import io.vena.bosk.annotations.ReferencePath;
import io.vena.bosk.drivers.mongo.MongoDriverSettings.MongoDriverSettingsBuilder;
import io.vena.bosk.drivers.state.TestEntity;
import io.vena.bosk.drivers.state.TestValues;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.vena.bosk.drivers.mongo.MainDriver.COLLECTION_NAME;
import static java.util.concurrent.TimeUnit.SECONDS;

abstract class AbstractMongoDriverTest {
	protected static final Identifier entity123 = Identifier.from("123");
	protected static final Identifier entity124 = Identifier.from("124");
	protected static final Identifier rootID = Identifier.from("root");

	protected static MongoService mongoService;
	protected DriverFactory<TestEntity> driverFactory;
	protected Deque<Runnable> tearDownActions;
	protected final MongoDriverSettings driverSettings;

	public AbstractMongoDriverTest(MongoDriverSettingsBuilder driverSettings) {
		this.driverSettings = driverSettings.build();
	}


	@BeforeAll
	public static void setupMongoConnection() {
		mongoService = new MongoService();
	}

	@BeforeEach
	void setupDriverFactory() {
		driverFactory = createDriverFactory();

		// Start with a clean slate
		mongoService.client()
			.getDatabase(driverSettings.database())
			.getCollection(COLLECTION_NAME)
			.drop();
	}

	@BeforeEach
	void clearTearDown(TestInfo testInfo) {
		logTest("/=== Start", testInfo);
		tearDownActions = new ArrayDeque<>();
//		tearDownActions.addLast(() ->  {
//			try {
//				LOGGER.debug("Sleeping after teardown");
//				Thread.sleep(10_000);
//			} catch (InterruptedException e) {
//				LOGGER.debug("Interrupted", e);
//				Thread.interrupted();
//			} finally {
//				LOGGER.debug("Done sleeping");
//			}
//		});
	}

	@AfterEach
	void runTearDown(TestInfo testInfo) {
		tearDownActions.forEach(Runnable::run);
		logTest("\\=== Done", testInfo);
	}

	// We'd like to use SLF4J's "Level" but that doesn't support OFF
	public void setLogging(Level level, Logger logger) {
		ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger) logger;
		if (Level.DEBUG.isGreaterOrEqual(logbackLogger.getEffectiveLevel())) {
			// Note: in logback, a "greater" level is less verbose. So if DEBUG is greater or equal to
			// the current effective level, it means the user has asked for DEBUG logs (or more),
			// assuming the logback.xml settings specify INFO by default.
			if (!ALREADY_WARNED.getAndSet(true)) {
				LOGGER.warn("Logging level is {}; ignoring the recommended setting from the testcase itself", logbackLogger.getEffectiveLevel());
			}
			return;
		}
		Level originalLevel = logbackLogger.getLevel();
		if (originalLevel == null) {
			tearDownActions.addFirst(()->logbackLogger.setLevel(originalLevel));
			logbackLogger.setLevel(level);
		} else if (!ALREADY_WARNED.getAndSet(true)){
			LOGGER.warn("Logging level has been overridden by the user; ignoring the recommended setting from the testcase itself");
		}
	}

	public void setLogging(Level level, Class<?>... loggers) {
		for (var logger: loggers) {
			setLogging(level, LoggerFactory.getLogger(logger));
		}
	}

	public void setLogging(Level level, Package logger) {
		setLogging(level, LoggerFactory.getLogger(logger.getName()));
	}

	private static void logTest(String verb, TestInfo testInfo) {
		String method =
			testInfo.getTestClass().map(Class::getSimpleName).orElse(null)
				+ "."
				+ testInfo.getTestMethod().map(Method::getName).orElse(null);
		LOGGER.info("{} {} {}", verb, method, testInfo.getDisplayName());
	}


	public TestEntity initialRoot(Bosk<TestEntity> testEntityBosk) throws InvalidTypeException {
		Refs refs = testEntityBosk.buildReferences(Refs.class);
		return initialRootWithEmptyCatalog(testEntityBosk)
			.withCatalog(Catalog.of(
				TestEntity.empty(entity123, refs.childCatalog(entity123)),
				TestEntity.empty(entity124, refs.childCatalog(entity124))
			));
	}

	public TestEntity initialRootWithValues(Bosk<TestEntity> testEntityBosk) throws InvalidTypeException {
		return initialRootWithEmptyCatalog(testEntityBosk)
			.withValues(Optional.of(TestValues.blank()));
	}

	public TestEntity initialRootWithEmptyCatalog(Bosk<TestEntity> testEntityBosk) throws InvalidTypeException {
		Refs refs = testEntityBosk.buildReferences(Refs.class);
		return new TestEntity(rootID,
			rootID.toString(),
			Catalog.empty(),
			Listing.of(refs.catalog(), entity123),
			SideTable.empty(refs.catalog()),
			Optional.empty()
		);
	}

	protected <E extends Entity> DriverFactory<E> createDriverFactory() {
		return (boskInfo, downstream) -> {
			MongoDriver<E> driver = MongoDriver.<E>factory(
				MongoClientSettings.builder(mongoService.clientSettings())
					.applyToClusterSettings(builder -> {
						builder.serverSelectionTimeout(5, SECONDS);
					})
					.applyToSocketSettings(builder -> {
						// We're testing timeouts. Let's not wait too long.
						builder.readTimeout(5, SECONDS);
					})
					.build(),
				driverSettings,
				new BsonPlugin()
			).build(boskInfo, downstream);
			tearDownActions.addFirst(driver::close);
			return driver;
		};
	}

	public interface Refs {
		@ReferencePath("/catalog") CatalogReference<TestEntity> catalog();
		@ReferencePath("/catalog/-child-/catalog") CatalogReference<TestEntity> childCatalog(Identifier child);
		@ReferencePath("/listing") ListingReference<TestEntity> listing();
		@ReferencePath("/listing/-entity-") Reference<ListingEntry> listingEntry(Identifier entity);
		@ReferencePath("/values") Reference<TestValues> values();
		@ReferencePath("/values/string") Reference<String> valuesString();
	}

	/**
	 * One warning that we're ignoring logging settings from the testcase is enough.
	 */
	private static final AtomicBoolean ALREADY_WARNED = new AtomicBoolean(false);
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMongoDriverTest.class);
}
