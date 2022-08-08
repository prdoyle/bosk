package org.vena.bosk.drivers;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.TracerProvider;
import org.junit.jupiter.api.BeforeEach;

class TracingDriverConformanceTest extends DriverConformanceTest {
	@BeforeEach
	void setupDriverFactory() {
		driverFactory = (downstream, bosk) ->
			new TracingDriver<>(
				TracerProvider.noop(), // If we do actual tracing, we'll interfere with TracingDriverTest
				Attributes.empty(),
				downstream);
	}
}
