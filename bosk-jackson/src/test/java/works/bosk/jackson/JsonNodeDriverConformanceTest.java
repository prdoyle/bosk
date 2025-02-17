package works.bosk.jackson;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.LoggerFactory;
import works.bosk.drivers.DriverConformanceTest;

// TODO: This currently doesn't test much beyond the driver's ability to pass updates downstream
class JsonNodeDriverConformanceTest extends DriverConformanceTest {
	@BeforeEach
	void setUp() {
		driverFactory = JsonNodeDriver.factory(new JacksonPlugin());
	}
}
