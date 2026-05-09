package works.bosk.jackson;

import org.junit.jupiter.api.BeforeEach;
import tools.jackson.databind.JsonNode;
import works.bosk.BoskDriver;
import works.bosk.junit.InjectFrom;
import works.bosk.testing.drivers.AbstractDriverTest.SingleTreeScenarioInjector;
import works.bosk.testing.drivers.DriverConformanceTest;
import works.bosk.testing.drivers.state.TestEntity;

import static org.junit.jupiter.api.Assertions.assertEquals;

@InjectFrom(SingleTreeScenarioInjector.class)
class JsonNodeDriverConformanceTest extends DriverConformanceTest {
	private JsonNodeDriver jsonNodeDriver;

	@BeforeEach
	void setUp() {
		driverFactory = (b,d) -> {
			BoskDriver result = JsonNodeDriver.<TestEntity>factory(new JacksonSerializer()).build(b, d);
			jsonNodeDriver = (JsonNodeDriver) result;
			return result;
		};
	}

	@Override
	protected void assertCorrectBoskContents() {
		super.assertCorrectBoskContents();
		JsonNode expected, actual;
		try (var _ = bosk.readSession()) {
			var root = bosk.rootReference().value();
			expected = jsonNodeDriver.mapper.convertValue(root, JsonNode.class);
			actual = jsonNodeDriver.currentRoot;
		}
		assertEquals(expected, actual);
	}
}
