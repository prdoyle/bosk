package io.vena.bosk.drivers;

import org.junit.jupiter.api.BeforeEach;

import static java.util.Collections.singletonList;

public class ForwardingDriverConformanceTest extends DriverConformanceTest {

	@BeforeEach
	void setupDriverFactory() {
		driverFactory = (r,d)-> new ForwardingDriver<>(singletonList(d));
	}

}
