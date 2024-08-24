package works.bosk.drivers;

import org.junit.jupiter.api.BeforeEach;

public class ForwardingDriverConformanceTest extends DriverConformanceTest {

	@BeforeEach
	void setupDriverFactory() {
		driverFactory = (_, d)-> new ForwardingDriver(d);
	}

}
