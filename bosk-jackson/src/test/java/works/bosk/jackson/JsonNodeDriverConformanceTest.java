package works.bosk.jackson;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.LoggerFactory;
import works.bosk.drivers.DriverConformanceTest;

class JsonNodeDriverConformanceTest extends DriverConformanceTest {

	@BeforeEach
	void setUp() {
//		Logger logger = (Logger) LoggerFactory.getLogger(JsonNodeDriver.class);
//		logger.setLevel(Level.TRACE);
		driverFactory = JsonNodeDriver.factory(new JacksonPlugin());
	}

}
