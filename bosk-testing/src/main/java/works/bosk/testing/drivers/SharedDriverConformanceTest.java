package works.bosk.testing.drivers;

import org.junit.jupiter.api.Test;
import works.bosk.Bosk;
import works.bosk.BoskConfig;
import works.bosk.BoskConfig.TenancyModel.Fixed;
import works.bosk.BoskConfig.TenancyModel.None;
import works.bosk.BoskConfig.TenancyModel.Persistent;
import works.bosk.BoskDriver.InitialState;
import works.bosk.Path;
import works.bosk.exceptions.TenancyShenanigansException;
import works.bosk.testing.BoskTestUtils;
import works.bosk.testing.drivers.state.TestEntity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static works.bosk.BoskConfig.TenancyModel.NONE;
import static works.bosk.BoskConfig.TenancyModel.PERSISTENT;

/**
 * Tests the ability of a driver to share state between two bosks.
 * <p>
 * Most such drivers should consider using {@link PolyfillDriverConformanceTest}
 * instead unless they can't support {@link works.bosk.annotations.Polyfill Polyfill}.
 */
public abstract class SharedDriverConformanceTest extends DriverConformanceTest {

	@Override
	protected void assertCorrectBoskContents() {
		super.assertCorrectBoskContents();
		var latecomer = new Bosk<>(
			BoskTestUtils.boskName("latecomer"),
			TestEntity.class,
			this::initialState,
			BoskConfig.<TestEntity>builder()
				.driverFactory(driverFactory)
				.tenancyModel(scenario.tenancyModel)
				.build());
		try {
			latecomer.driver().flush();
		} catch (Exception e) {
			throw new AssertionError("Unexpected exception", e);
		}
		InitialState<TestEntity> expected, actual;
		try (
			var _ = canonicalBosk.readSession()
		) {
			expected = canonicalBosk.entireState();
		}
		try (
			var _ = latecomer.readSession()
		) {
			actual = latecomer.entireState();
		}
		assertEquals(expected, actual);
	}

	@Test
	void wrongTenancy_disallowed() {
		// Initialize bosk
		super.startingState(Path.just(TestEntity.Fields.catalog));

		// Try to make an incompatible one
		switch (scenario.tenancyModel) {
			case None _ -> {
				assertThrows(TenancyShenanigansException.class, () -> new Bosk<>(
					BoskTestUtils.boskName("latecomer"),
					TestEntity.class,
					this::initialState,
					BoskConfig.<TestEntity>builder()
						.driverFactory(driverFactory)
						.tenancyModel(PERSISTENT)
						.build()
				), "Tenancy model None is incompatible with Persistent");
			}
			case Fixed _ -> {
				// It's unclear whether we want to mandate that all shared drivers
				// must detect and report this. Example: a database-backed driver
				// might be in the early stages of transitioning to multitenancy,
				// and might initially still be in a "no tenancy" mode. Forcing the
				// "no tenancy" mode to detect tenant mismatches seems wrong somehow.
//					assertThrows(TenancyShenanigansException.class, () -> new Bosk<>(
//						BoskTestUtils.boskName("latecomer"),
//						TestEntity.class,
//						this::initialState,
//						BoskConfig.<TestEntity>builder()
//							.driverFactory(driverFactory)
//							.tenancyModel(new Fixed(Identifier.from("NOT " + scenario.automaticallyEstablishedTenant())))
//							.build()
//					), "Tenancy model Fixed is incompatible with a different tenant ID");
			}
			case Persistent _ -> {
				assertThrows(TenancyShenanigansException.class, () -> new Bosk<>(
					BoskTestUtils.boskName("latecomer"),
					TestEntity.class,
					this::initialState,
					BoskConfig.<TestEntity>builder()
						.driverFactory(driverFactory)
						.tenancyModel(NONE)
						.build()
				), "Tenancy model Persistent is incompatible with None");
			}
		}
	}
}
