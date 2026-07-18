package works.bosk.testing.drivers;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import works.bosk.Bosk;
import works.bosk.BoskConfig;
import works.bosk.BoskConfig.TenancyModel.Explicit;
import works.bosk.BoskConfig.TenancyModel.Fixed;
import works.bosk.BoskContext.Tenant;
import works.bosk.BoskContext.Tenant.TenantId;
import works.bosk.BoskDriver.EntireState;
import works.bosk.DriverFactory;
import works.bosk.testing.drivers.state.TestEntity;
import works.bosk.util.PerTenantValue;

import static java.util.function.Function.identity;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static works.bosk.testing.BoskTestUtils.boskName;

/**
 * Tests the ability of a driver to share state between multiple Bosks.
 * <p>
 * Each call to {@link #assertCorrectBoskContents()} checks two additional
 * bosks: one long-lived one, and one that is newly created immediately before
 * the assertion.
 */
public abstract class SharedDriverConformanceTest extends DriverConformanceTest {
	Bosk<TestEntity> remoteBosk;

	@Override
	protected void setupBosksAndReferences(DriverFactory<TestEntity> driverFactory) {
		super.setupBosksAndReferences(driverFactory);
		remoteBosk = new Bosk<>(
			boskName("remote"),
			TestEntity.class,
			this::initialState,
			BoskConfig.<TestEntity>builder()
				.driverFactory(driverFactory)
				.tenancyModel(scenario.tenancyModel)
				.build());
	}

	@Override
	protected void assertCorrectBoskContents() {
		super.assertCorrectBoskContents();
		assertSameBoskContents(remoteBosk);

		var latecomer = new Bosk<>(
			boskName("latecomer"),
			TestEntity.class,
			this::initialState,
			BoskConfig.<TestEntity>builder()
				.driverFactory(driverFactory)
				.tenancyModel(scenario.tenancyModel)
				.build());
		assertSameBoskContents(latecomer);
	}

	private void assertSameBoskContents(Bosk<TestEntity> otherBosk) {
		try {
			otherBosk.driver().flush();
		} catch (Exception e) {
			throw new AssertionError("Unexpected exception", e);
		}
		EntireState<TestEntity> expected, actual;
		try (
			var _ = canonicalBosk.readSession()
		) {
			expected = canonicalBosk.entireState();
		}
		try (
			var _ = otherBosk.readSession()
		) {
			actual = otherBosk.entireState();
		}
		if (scenario.tenancyModel instanceof Fixed(var id)) {
			// Either a single tenant or NoTenant is ok. Normalize before comparing
			TenantId tenantId = Tenant.setTo(id);
			var e = PerTenantValue.from(expected, identity()).asNoTenant(tenantId);
			var a = PerTenantValue.from(actual, identity()).asNoTenant(tenantId);
			assertEquals(e, a);
		} else {
			// Must be exactly equal
			assertEquals(expected, actual);
		}
	}

	/**
	 * Don't get confused! This test has two bosks <em>and</em> two tenants.
	 * <p>
	 * The single-bosk version of this test would also be valid,
	 * but this particular one is a regression test for a bug that required two bosks.
	 */
	@Test
	void multiTenant_unchangedDiagnostics_propagateCorrectly() throws Exception {
		setupBosksAndReferences(driverFactory);
		assumeTrue(scenario.tenancyModel instanceof Explicit);
		closeTenantScope();

		Refs refs = bosk.buildReferences(Refs.class);
		Refs remoteRefs = remoteBosk.buildReferences(Refs.class);
		AtomicBoolean hooksArmed = new AtomicBoolean(false);

		bosk.hookRegistrar().registerHook("Verify attribute value", refs.string(), _ -> {
			if (hooksArmed.get()) {
				var tenant = bosk.context().getTenantId();
				assertEquals(tenant.tenant().toString(), bosk.context().getAttribute("testKey"));
			}
		});
		remoteBosk.hookRegistrar().registerHook("Verify attribute value", remoteRefs.string(), _ -> {
			if (hooksArmed.get()) {
				var tenant = remoteBosk.context().getTenantId();
				assertEquals(tenant.tenant().toString(), remoteBosk.context().getAttribute("testKey"));
			}
		});

		hooksArmed.set(true);

		for (int i = 1; i <= 3; i++) {
			try (
				var _ = bosk.context().withTenant(tenant1);
				var _ = bosk.context().withAttribute("testKey", tenant1.tenant().toString())
			) {
				driver.submitReplacement(refs.string(), "tenant1-update" + i);
			}
			try (
				var _ = bosk.context().withTenant(tenant2);
				var _ = bosk.context().withAttribute("testKey", tenant2.tenant().toString())
			) {
				driver.submitReplacement(refs.string(), "tenant2-update" + i);
			}
		}
		bosk.driver().flush();
		remoteBosk.driver().flush();

		assertCorrectBoskContents();
	}

}
