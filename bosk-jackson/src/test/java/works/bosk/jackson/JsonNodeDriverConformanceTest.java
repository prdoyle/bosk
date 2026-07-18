package works.bosk.jackson;

import org.junit.jupiter.api.BeforeEach;
import tools.jackson.databind.JsonNode;
import works.bosk.BoskDriver;
import works.bosk.BoskDriver.EntireState.MultiTree;
import works.bosk.BoskDriver.EntireState.SingleTree;
import works.bosk.testing.drivers.DriverConformanceTest;
import works.bosk.testing.drivers.state.TestEntity;
import works.bosk.util.PerTenantValue;
import works.bosk.util.PerTenantValue.MultiTenant;
import works.bosk.util.PerTenantValue.NoTenant;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
		PerTenantValue<JsonNode> expected, actual;
		try (var _ = bosk.readSession()) {
			var state = bosk.entireState();
			expected = switch (state) {
				case SingleTree(var root) -> NoTenant.just(jsonNodeDriver.mapper.convertValue(root, JsonNode.class));
				case MultiTree(var roots) -> roots.entrySet().stream()
					.collect(MultiTenant.withValues(v -> jsonNodeDriver.mapper.convertValue(v, JsonNode.class)));
			};
			actual = jsonNodeDriver.contents;
		}
		assertEquals(expected, actual);
	}
}
