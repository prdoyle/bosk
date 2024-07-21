package works.bosk.jackson;

import org.junit.jupiter.api.BeforeEach;
import works.bosk.drivers.DriverConformanceTest;

import static works.bosk.AbstractRoundTripTest.jacksonRoundTripFactory;

public class JacksonRoundTripConformanceTest extends DriverConformanceTest {
	@BeforeEach
	void setupDriverFactory() {
		driverFactory = jacksonRoundTripFactory();
	}
}
