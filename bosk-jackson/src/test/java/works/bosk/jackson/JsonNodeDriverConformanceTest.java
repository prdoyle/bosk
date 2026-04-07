package works.bosk.jackson;

import org.junit.jupiter.api.BeforeEach;
import works.bosk.junit.InjectFrom;
import works.bosk.testing.drivers.AbstractDriverTest.SingleTreeScenarioInjector;
import works.bosk.testing.drivers.DriverConformanceTest;

// TODO: This currently doesn't test much beyond the driver's ability to pass updates downstream
@InjectFrom(SingleTreeScenarioInjector.class)
class JsonNodeDriverConformanceTest extends DriverConformanceTest {
	@BeforeEach
	void setUp() {
		driverFactory = JsonNodeDriver.factory(new JacksonSerializer());
	}
}
