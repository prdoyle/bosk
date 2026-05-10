package works.bosk.jackson;

import java.util.Map.Entry;
import org.junit.jupiter.api.BeforeEach;
import tools.jackson.databind.JsonNode;
import works.bosk.BoskDriver;
import works.bosk.BoskDriver.InitialState;
import works.bosk.BoskDriver.InitialState.SingleTree;
import works.bosk.jackson.JsonNodeDriver.Contents;
import works.bosk.testing.drivers.DriverConformanceTest;
import works.bosk.testing.drivers.state.TestEntity;

import static java.util.stream.Collectors.toMap;
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
		Contents expected, actual;
		try (var _ = bosk.readSession()) {
			var state = bosk.entireState();
			expected = switch (state) {
				case SingleTree(var root) -> new Contents.SingleTree(jsonNodeDriver.mapper.convertValue(root, JsonNode.class));
				case InitialState.MultiTree(var roots) -> new Contents.MultiTree(roots.entrySet().stream().collect(toMap(
					Entry::getKey,
					e -> jsonNodeDriver.mapper.convertValue(e.getValue(), JsonNode.class),
					(_,b) -> b,
					java.util.TreeMap::new
				)));
			};
			actual = jsonNodeDriver.contents;
		}
		assertEquals(expected, actual);
	}
}
