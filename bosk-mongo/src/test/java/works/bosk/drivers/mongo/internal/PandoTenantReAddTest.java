package works.bosk.drivers.mongo.internal;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import works.bosk.Bosk;
import works.bosk.BoskConfig;
import works.bosk.BoskConfig.TenancyModel;
import works.bosk.BoskContext.Tenant;
import works.bosk.BoskContext.Tenant.TenantId;
import works.bosk.BoskDriver;
import works.bosk.BoskDriver.EntireState;
import works.bosk.DriverFactory;
import works.bosk.DriverStack;
import works.bosk.Identifier;
import works.bosk.Path;
import works.bosk.drivers.mongo.BsonSerializer;
import works.bosk.drivers.mongo.MongoDriver;
import works.bosk.drivers.mongo.MongoDriverSettings;
import works.bosk.drivers.mongo.PandoFormat;
import works.bosk.logback.BoskLogFilter;
import works.bosk.logback.ReplayLogsOnFailure;
import works.bosk.testing.drivers.state.TestEntity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static works.bosk.drivers.mongo.MongoDriverSettings.TenancyFormat.ID_PREFIX;
import static works.bosk.drivers.mongo.internal.MainDriver.COLLECTION_NAME;
import static works.bosk.testing.BoskTestUtils.boskName;

/**
 * Tests tenant add/remove/re-add cycles in {@link PandoFormatDriver}.
 * <p>
 * {@link PandoFormatDriver#doReplacement} re-adds a tenant by writing its document at revision 1
 * ({@code initializeTenant(tid, value, nextRevision(REVISION_ZERO))}), regardless of what
 * revision the orphaned document (left behind by the earlier removal, per HASTY orphan mode) was
 * already at. The author of that code was aware re-adds need help getting past the driver's own
 * {@code FlushLock} skip check, and patches the WRITER's own lock at write time
 * ({@code finishedRevision(tid, REVISION_BEFORE_ANY)}, {@code PandoFormatDriver.java:880}).
 * <p>
 * But that patch is applied at WRITE time, while the skip decision happens at EVENT-PROCESSING
 * time. If the application performs additional writes (here: remove then re-add) before the
 * change stream has processed the earlier ones -- the normal case, since writes do not wait for
 * their own events -- those intervening events raise the per-tenant lock past the value the
 * patch set, and the re-add's change-stream event (revision 1) is skipped as stale
 * ({@code shouldSkip}: {@code revision <= alreadySeen}). The tenant is lost permanently: no
 * further event will ever un-skip it, because its revision numbers were reused rather than
 * carried forward.
 */
@ReplayLogsOnFailure
class PandoTenantReAddTest extends AbstractMongoDriverTest {
	PandoTenantReAddTest() {
		super(MongoDriverSettings.builder()
			.database(PandoTenantReAddTest.class.getSimpleName())
			.preferredDatabaseFormat(PandoFormat.oneBigDocument().withTenancyFormat(ID_PREFIX))
			.timescaleMS(5_000));
	}

	@Test
	void reAddTenant_afterRemove_isNotLost(TestInfo testInfo) throws Exception {
		var racer = newBosk(testInfo, "ReAddRace",
			MongoDriverSettings.Testing.builder().eventDelayMS(200).build());
		addRemoveReAddOrFail(racer.bosk, racer.driver, "reAdd");
	}

	@Test
	void addRemoveReAdd_noEventDelay(TestInfo testInfo) throws Exception {
		var racer = newBosk(testInfo, "NoDelay");
		addRemoveReAddOrFail(racer.bosk, racer.driver, "reAdd");
	}

	@Test
	void addRemoveReAdd_twoTenants(TestInfo testInfo) throws Exception {
		TenantId tenantA = Tenant.setTo(Identifier.from("tenantA"));
		TenantId tenantB = Tenant.setTo(Identifier.from("tenantB"));

		var racer = newBosk(testInfo, "TwoTenants");
		Bosk<TestEntity> bosk = racer.bosk;
		BoskDriver driver = racer.driver;

		TestEntity rootA;
		TestEntity rootB;
		try (var _ = bosk.context().withTenant(tenantA)) {
			rootA = TestEntity.empty(Identifier.from("tenantA"),
				bosk.rootReference().thenCatalog(TestEntity.class, Path.just(TestEntity.Fields.catalog)));
			LOGGER.debug("Add tenantA");
			driver.submitConditionalCreation(bosk.rootReference(), rootA);
		}
		try (var _ = bosk.context().withTenant(tenantB)) {
			rootB = TestEntity.empty(Identifier.from("tenantB"),
				bosk.rootReference().thenCatalog(TestEntity.class, Path.just(TestEntity.Fields.catalog)));
			LOGGER.debug("Add tenantB");
			driver.submitConditionalCreation(bosk.rootReference(), rootB);
		}
		try (var _ = bosk.context().withTenant(tenantA)) {
			LOGGER.debug("Remove tenantA");
			driver.submitDeletion(bosk.rootReference());
		}
		try (var _ = bosk.context().withTenant(tenantA)) {
			TestEntity reAdd = rootA.withString("reAdded");
			LOGGER.debug("Re-add tenantA");
			driver.submitConditionalCreation(bosk.rootReference(), reAdd);
		}

		driver.flush();

		try (var _ = bosk.context().withTenant(tenantB); var _ = bosk.readSession()) {
			assertNotNull(bosk.rootReference().valueIfExists(), "tenantB should still exist");
		}
		try (var _ = bosk.context().withTenant(tenantA); var _ = bosk.readSession()) {
			assertEquals("reAdded", bosk.rootReference().valueIfExists().string(),
				"Re-added tenantA should have the re-added value");
		}
	}

	@Test
	void addRemoveReAdd_andReplace(TestInfo testInfo) throws Exception {
		TenantId tenantA = Tenant.setTo(Identifier.from("tenantA"));

		var racer = newBosk(testInfo, "FullLifecycle");
		Bosk<TestEntity> bosk = racer.bosk;
		BoskDriver driver = racer.driver;

		TestEntity rootA, reAdd, replaced;
		try (var _ = bosk.context().withTenant(tenantA)) {
			rootA = TestEntity.empty(Identifier.from("tenantA"),
				bosk.rootReference().thenCatalog(TestEntity.class, Path.just(TestEntity.Fields.catalog)));
			LOGGER.debug("Add tenantA");
			driver.submitConditionalCreation(bosk.rootReference(), rootA);

			LOGGER.debug("Remove tenantA");
			driver.submitDeletion(bosk.rootReference());

			reAdd = rootA.withString("reAdded");
			LOGGER.debug("Re-add tenantA");
			driver.submitConditionalCreation(bosk.rootReference(), reAdd);

			replaced = reAdd.withString("replaced");
			LOGGER.debug("Replace tenantA root");
			driver.submitReplacement(bosk.rootReference(), replaced);
		}

		driver.flush();

		try (var _ = bosk.context().withTenant(tenantA); var _ = bosk.readSession()) {
			assertEquals("replaced", bosk.rootReference().valueIfExists().string(),
				"TenantA should have the replacement value");
		}
	}

	@Test
	void replacementAfterRemove_reEnlists(TestInfo testInfo) throws Exception {
		TenantId tenantA = Tenant.setTo(Identifier.from("tenantA"));

		var racer = newBosk(testInfo, "ReplacementReEnlist");
		Bosk<TestEntity> bosk = racer.bosk;
		BoskDriver driver = racer.driver;

		TestEntity rootA, replacement;
		try (var _ = bosk.context().withTenant(tenantA)) {
			rootA = TestEntity.empty(Identifier.from("tenantA"),
				bosk.rootReference().thenCatalog(TestEntity.class, Path.just(TestEntity.Fields.catalog)));
			LOGGER.debug("Add tenantA");
			driver.submitConditionalCreation(bosk.rootReference(), rootA);

			LOGGER.debug("Remove tenantA");
			driver.submitDeletion(bosk.rootReference());

			replacement = rootA.withString("replacement");
			LOGGER.debug("SubmitReplacement on removed tenantA");
			driver.submitReplacement(bosk.rootReference(), replacement);
		}

		driver.flush();

		try (var _ = bosk.context().withTenant(tenantA); var _ = bosk.readSession()) {
			assertEquals("replacement", bosk.rootReference().valueIfExists().string(),
				"TenantA should be re-enlisted with the replacement value");
		}
	}

	@Test
	void registerCreatedTenantInContents(TestInfo testInfo) throws Exception {
		// Ensures that creating a tenant using submitConditionalCreation registers it in !contents
		TenantId tenantA = Tenant.setTo(Identifier.from("tenantA"));

		var racer = newBosk(testInfo, "RegisterInContents");
		Bosk<TestEntity> bosk = racer.bosk;
		BoskDriver driver = racer.driver;

		try (var _ = bosk.context().withTenant(tenantA)) {
			var rootA = TestEntity.empty(Identifier.from("tenantA"),
				bosk.rootReference().thenCatalog(TestEntity.class, Path.just(TestEntity.Fields.catalog)));
			LOGGER.debug("Add tenantA");
			driver.submitConditionalCreation(bosk.rootReference(), rootA);
		}

		driver.flush();

		try (var _ = bosk.context().withTenant(tenantA); var _ = bosk.readSession()) {
			assertNotNull(bosk.rootReference().valueIfExists(), "Newly created tenant should be visible");
		}
	}

	@Test
	void addRemoveFlushReAdd(TestInfo testInfo) throws Exception {
		TenantId tenantA = Tenant.setTo(Identifier.from("tenantA"));

		var racer = newBosk(testInfo, "FlushBetween");
		Bosk<TestEntity> bosk = racer.bosk;
		BoskDriver driver = racer.driver;

		TestEntity rootA, reAdd;
		try (var _ = bosk.context().withTenant(tenantA)) {
			rootA = TestEntity.empty(Identifier.from("tenantA"),
				bosk.rootReference().thenCatalog(TestEntity.class, Path.just(TestEntity.Fields.catalog)));
			LOGGER.debug("Add tenantA");
			driver.submitConditionalCreation(bosk.rootReference(), rootA);
		}

		// Flush so the change stream processes the add before we remove
		driver.flush();

		try (var _ = bosk.context().withTenant(tenantA)) {
			LOGGER.debug("Remove tenantA");
			driver.submitDeletion(bosk.rootReference());
		}

		// Flush so the change stream processes the deletion before we re-add
		driver.flush();

		try (var _ = bosk.context().withTenant(tenantA)) {
			reAdd = rootA.withString("reAdded");
			LOGGER.debug("Re-add tenantA");
			driver.submitConditionalCreation(bosk.rootReference(), reAdd);
		}

		driver.flush();

		try (var _ = bosk.context().withTenant(tenantA); var _ = bosk.readSession()) {
			assertEquals("reAdded", bosk.rootReference().valueIfExists().string(),
				"Re-added tenant should be visible with its re-added value after flush");
		}
	}

	@Test
	void conditionalReplacement_againstDeadTenant_doesNotResurrect(TestInfo testInfo) throws Exception {
		TenantId tenantA = Tenant.setTo(Identifier.from("tenantA"));

		var racer = newBosk(testInfo, "CondReplaceDead");
		Bosk<TestEntity> bosk = racer.bosk;
		BoskDriver driver = racer.driver;

		try (var _ = bosk.context().withTenant(tenantA)) {
			var rootA = TestEntity.empty(Identifier.from("tenantA"),
				bosk.rootReference().thenCatalog(TestEntity.class, Path.just(TestEntity.Fields.catalog)));
			LOGGER.debug("Add tenantA");
			driver.submitConditionalCreation(bosk.rootReference(), rootA);
		}

		driver.flush();

		try (var _ = bosk.context().withTenant(tenantA)) {
			LOGGER.debug("Remove tenantA");
			driver.submitDeletion(bosk.rootReference());
		}

		driver.flush();

		// Now tenantA is dead but orphan root document exists with state.id = "tenantA"

		// Baseline orphan revision numbers
		var coll = mongoService.client()
			.getDatabase(driverSettings.database())
			.getCollection(COLLECTION_NAME, BsonDocument.class);
		long contentsRevBefore = readRevision(coll, "!contents");
		long rootRevBefore = readRevision(coll, "<tenantA>|");

		// Conditional replacement with matching precondition on dead tenant
		try (var _ = bosk.context().withTenant(tenantA)) {
			var precondition = bosk.rootReference().then(Identifier.class, Path.just(TestEntity.Fields.id));
			var newValue = TestEntity.empty(Identifier.from("tenantA"),
				bosk.rootReference().thenCatalog(TestEntity.class, Path.just(TestEntity.Fields.catalog)));
			LOGGER.debug("Conditional replacement on dead tenant with matching precondition");
			driver.submitConditionalReplacement(bosk.rootReference(), newValue, precondition, Identifier.from("tenantA"));
		}

		// Read revisions after (the write should not happen with the fix)
		long contentsRevAfter = readRevision(coll, "!contents");
		long rootRevAfter = readRevision(coll, "<tenantA>|");

		assertEquals(contentsRevBefore, contentsRevAfter,
			"!contents revision should not change after conditional replacement on dead tenant");
		assertEquals(rootRevBefore, rootRevAfter,
			"Orphan root document revision should not change after conditional replacement on dead tenant");
	}

	@Test
	void conditionalDeletion_againstDeadTenant_doesNotMutateOrphan(TestInfo testInfo) throws Exception {
		TenantId tenantA = Tenant.setTo(Identifier.from("tenantA"));

		var racer = newBosk(testInfo, "CondDeleteDead");
		Bosk<TestEntity> bosk = racer.bosk;
		BoskDriver driver = racer.driver;

		try (var _ = bosk.context().withTenant(tenantA)) {
			var rootA = TestEntity.empty(Identifier.from("tenantA"),
				bosk.rootReference().thenCatalog(TestEntity.class, Path.just(TestEntity.Fields.catalog)));
			LOGGER.debug("Add tenantA");
			driver.submitConditionalCreation(bosk.rootReference(), rootA);
		}

		driver.flush();

		try (var _ = bosk.context().withTenant(tenantA)) {
			LOGGER.debug("Remove tenantA");
			driver.submitDeletion(bosk.rootReference());
		}

		driver.flush();

		// Baseline orphan revision numbers
		var coll = mongoService.client()
			.getDatabase(driverSettings.database())
			.getCollection(COLLECTION_NAME, BsonDocument.class);
		long contentsRevBefore = readRevision(coll, "!contents");
		long rootRevBefore = readRevision(coll, "<tenantA>|");

		// Conditional deletion with matching precondition on dead tenant
		try (var _ = bosk.context().withTenant(tenantA)) {
			var precondition = bosk.rootReference().then(Identifier.class, Path.just(TestEntity.Fields.id));
			LOGGER.debug("Conditional deletion on dead tenant with matching precondition");
			driver.submitConditionalDeletion(bosk.rootReference(), precondition, Identifier.from("tenantA"));
		}

		// Read revisions after (the write should not happen with the fix)
		long contentsRevAfter = readRevision(coll, "!contents");
		long rootRevAfter = readRevision(coll, "<tenantA>|");

		assertEquals(contentsRevBefore, contentsRevAfter,
			"!contents revision should not change after conditional deletion on dead tenant");
		assertEquals(rootRevBefore, rootRevAfter,
			"Orphan root document revision should not change after conditional deletion on dead tenant");
	}

	private static long readRevision(MongoCollection<BsonDocument> coll, String documentId) {
		var doc = coll.find(Filters.eq("_id", documentId)).first();
		if (doc == null) {
			return -1;
		}
		return doc.getInt64("revision", new BsonInt64(0)).longValue();
	}

	/**
	 * Helper: add, remove, re-add tenantA, flush, and assert the re-added value.
	 */
	private void addRemoveReAddOrFail(Bosk<TestEntity> bosk, BoskDriver driver, String reAddString) throws Exception {
		TenantId tenantA = Tenant.setTo(Identifier.from("tenantA"));

		TestEntity firstAdd, reAdd;
		try (var _ = bosk.context().withTenant(tenantA)) {
			firstAdd = TestEntity.empty(Identifier.from("tenantA"),
				bosk.rootReference().thenCatalog(TestEntity.class, Path.just(TestEntity.Fields.catalog)));
			LOGGER.debug("Add tenantA");
			driver.submitConditionalCreation(bosk.rootReference(), firstAdd);

			LOGGER.debug("Remove tenantA");
			driver.submitDeletion(bosk.rootReference());

			reAdd = firstAdd.withString(reAddString);
			LOGGER.debug("Re-add tenantA");
			driver.submitConditionalCreation(bosk.rootReference(), reAdd);
		}

		driver.flush();
		TestEntity actual;
		try (var _ = bosk.context().withTenant(tenantA); var _ = bosk.readSession()) {
			actual = bosk.rootReference().valueIfExists();
		}
		assertEquals(reAdd, actual, "Re-added tenant should be visible with its latest value after flush");
	}

	/**
	 * Creates a Bosk for tenant re-add testing with custom testing settings.
	 */
	private BoskAndDriver newBosk(TestInfo testInfo, String boskNameSuffix,
		MongoDriverSettings.Testing testing) {
		var customSettings = MongoDriverSettings.builder()
			.database(PandoTenantReAddTest.class.getSimpleName())
			.preferredDatabaseFormat(PandoFormat.oneBigDocument().withTenancyFormat(ID_PREFIX))
			.timescaleMS(5_000)
			.testing(testing)
			.build();
		return newBoskWithSettings(testInfo, boskNameSuffix, customSettings);
	}

	/**
	 * Creates a Bosk for tenant re-add testing with default (no event delay) settings.
	 */
	private BoskAndDriver newBosk(TestInfo testInfo, String boskNameSuffix) {
		return newBoskWithSettings(testInfo, boskNameSuffix, driverSettings);
	}

	private BoskAndDriver newBoskWithSettings(TestInfo testInfo, String boskNameSuffix,
		MongoDriverSettings settings) {
		DriverFactory<TestEntity> factory = (boskInfo, downstream) -> {
			MongoDriver driver = MongoDriver.<TestEntity>factory(
				mongoService.clientSettings(testInfo),
				settings,
				new BsonSerializer()
			).build(boskInfo, downstream);
			tearDownActions.addFirst(driver::close);
			return driver;
		};
		factory = DriverStack.of(
			BoskLogFilter.withController(logController),
			factory
		);
		Bosk<TestEntity> bosk = new Bosk<>(
			boskName(boskNameSuffix),
			TestEntity.class,
			PandoTenantReAddTest::emptyMultiTree,
			BoskConfig.<TestEntity>builder()
				.tenancyModel(TenancyModel.EXPLICIT)
				.driverFactory(factory)
				.build());
		return new BoskAndDriver(bosk, bosk.driver());
	}

	private record BoskAndDriver(Bosk<TestEntity> bosk, BoskDriver driver) {}

	private static EntireState<TestEntity> emptyMultiTree(Bosk<TestEntity> bosk) {
		return EntireState.MultiTree.empty();
	}

	private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(PandoTenantReAddTest.class);
}
