package io.vena.bosk.drivers;

import org.junit.jupiter.api.BeforeEach;

public class BufferingDriverConformanceTest extends DriverConformanceTest {

	@BeforeEach
	void setupDriverFactory() {
		driverFactory = (r,d)-> BufferingDriver.writingTo(d);
	}

}
