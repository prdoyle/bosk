package works.bosk.drivers;

import works.bosk.Bosk;
import works.bosk.BoskTestUtils;
import works.bosk.drivers.state.TestEntity;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests the ability of a driver to share state between two bosks.
 */
public class SharedDriverConformanceTest extends DriverConformanceTest {

	@Override
	protected void assertCorrectBoskContents() {
		super.assertCorrectBoskContents();
		var latecomer = new Bosk<>(BoskTestUtils.boskName("latecomer"), TestEntity.class, AbstractDriverTest::initialRoot, driverFactory);
		try {
			latecomer.driver().flush();
		} catch (Exception e) {
			throw new AssertionError("Unexpected exception", e);
		}
		TestEntity expected, actual;
		try (var __ = canonicalBosk.readContext()) {
			expected = canonicalBosk.rootReference().value();
		}
		try (var __ = latecomer.readContext()) {
			actual = latecomer.rootReference().value();
		}
		assertEquals(expected, actual);
	}

}
