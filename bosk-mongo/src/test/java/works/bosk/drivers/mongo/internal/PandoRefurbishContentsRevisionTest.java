package works.bosk.drivers.mongo.internal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import works.bosk.Bosk;
import works.bosk.BoskConfig;
import works.bosk.BoskConfig.TenancyModel;
import works.bosk.BoskContext.Tenant;
import works.bosk.BoskContext.Tenant.TenantId;
import works.bosk.BoskDriver;
import works.bosk.BoskDriver.EntireState;
import works.bosk.Identifier;
import works.bosk.Path;
import works.bosk.drivers.mongo.MongoDriver;
import works.bosk.drivers.mongo.MongoDriverSettings;
import works.bosk.drivers.mongo.PandoFormat;
import works.bosk.logback.ReplayLogsOnFailure;
import works.bosk.testing.drivers.state.TestEntity;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static works.bosk.drivers.mongo.MongoDriverSettings.TenancyFormat.ID_PREFIX;
import static works.bosk.drivers.mongo.internal.TestParameters.SHORT_TIMESCALE;
import static works.bosk.testing.BoskTestUtils.boskName;

/**
 * Demonstrates a bug found by TLA+ modeling of {@link PandoFormatDriver}: refurbishing to the
 * SAME database format resets the {@code !contents} document's revision number back to 1
 * ({@code PandoFormatDriver#writeContentsDocument}, via {@code nextRevision(REVISION_ZERO)}),
 * instead of carrying the prior revision forward the way per-tenant state documents already do.
 * <p>
 * Because a same-format refurbish leaves the manifest unchanged, an observing replica does NOT
 * reconnect (its manifest change event is treated as benign), so it keeps its existing
 * {@code contentsFlushLock}. If that replica has already seen a {@code !contents} revision
 * number at least as high as the one a subsequent write receives after the reset, the observing
 * replica's {@code handleContentsEvent} skips the event entirely
 * ({@code contentsFlushLock.alreadySeen(revision)}), including a tenant REMOVAL, which is
 * propagated {@code ONLY} via that event. The result: the observing replica continues to serve a
 * tenant that has been deleted from the database.
 */
@ReplayLogsOnFailure
class PandoRefurbishContentsRevisionTest extends AbstractMongoDriverTest {
	PandoRefurbishContentsRevisionTest() {
		super(MongoDriverSettings.builder()
			.database(PandoRefurbishContentsRevisionTest.class.getSimpleName())
			.preferredDatabaseFormat(PandoFormat.oneBigDocument().withTenancyFormat(ID_PREFIX))
//			.testing(Testing.builder().eventDelayMS(SHORT_TIMESCALE).build())
			.timescaleMS(SHORT_TIMESCALE));
	}

	@Test
	void refurbish_doesNotLoseSubsequentTenantRemoval(TestInfo testInfo) throws Exception {
		TenantId tenantA = Tenant.setTo(Identifier.from("tenantA"));

		// The writer bosk initializes the (empty) collection and performs all the writes.
		Bosk<TestEntity> writerBosk = new Bosk<>(
			boskName("Writer"),
			TestEntity.class,
			PandoRefurbishContentsRevisionTest::emptyMultiTree,
			BoskConfig.<TestEntity>builder()
				.tenancyModel(TenancyModel.EXPLICIT)
				.driverFactory(driverFactory)
				.build());
		BoskDriver writerDriver = writerBosk.driver();

		// The observer bosk is an independent connection to the same collection that never writes.
		Bosk<TestEntity> observerBosk = new Bosk<>(
			boskName("Observer"),
			TestEntity.class,
			_ -> { throw new AssertionError("observerBosk should use the state from MongoDB"); },
			BoskConfig.<TestEntity>builder()
				.tenancyModel(TenancyModel.EXPLICIT)
				.driverFactory(createDriverFactory(logController, testInfo))
				.build());
		BoskDriver observerDriver = observerBosk.driver();

		LOGGER.debug("Writer adds tenantA");
		TestEntity rootA = TestEntity.empty(Identifier.from("tenantA"),
			writerBosk.rootReference().thenCatalog(TestEntity.class, Path.just(TestEntity.Fields.catalog)));
		try (
			var _ = writerBosk.context().withTenant(tenantA)
		) {
			writerDriver.submitConditionalCreation(writerBosk.rootReference(), rootA);
		}

		LOGGER.debug("Observer catches up, establishing its contentsFlushLock at the post-add revision");
		observerDriver.flush();
		try (
			var _ = observerBosk.context().withTenant(tenantA);
			var _ = observerBosk.readSession()
		) {
			assertNotNull(observerBosk.rootReference().valueIfExists(), "Observer should see the newly added tenant");
		}

		LOGGER.debug("Refurbish to the same format");
		writerBosk.getDriver(MongoDriver.class).refurbish();

		LOGGER.debug("Writer removes tenantA");
		try (
			var _ = writerBosk.context().withTenant(tenantA)
		) {
			writerDriver.submitDeletion(writerBosk.rootReference());
		}

		LOGGER.debug("Observer flush");
		observerDriver.flush();

		LOGGER.debug("Observer should no longer see tenantA");
		TestEntity leaked;
		try (
			var _ = observerBosk.context().withTenant(tenantA);
			var _ = observerBosk.readSession()
		) {
			leaked = observerBosk.rootReference().valueIfExists();
		}
		assertNull(leaked, "Removed tenant must not still be served after refurbish resets the !contents revision");
	}

	private static EntireState<TestEntity> emptyMultiTree(Bosk<TestEntity> bosk) {
		return EntireState.MultiTree.empty();
	}

	private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(PandoRefurbishContentsRevisionTest.class);
}
