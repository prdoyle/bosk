package works.bosk.jackson;

import java.util.stream.Stream;
import works.bosk.drivers.DriverConformanceTest;
import works.bosk.junit.ParametersByName;

import static works.bosk.AbstractRoundTripTest.jacksonRoundTripFactory;

public class JacksonRoundTripConformanceTest extends DriverConformanceTest {
	@ParametersByName
	JacksonRoundTripConformanceTest(JacksonPluginConfiguration config) {
		driverFactory = jacksonRoundTripFactory(config);
	}

	static Stream<JacksonPluginConfiguration> config() {
		return Stream.of(JacksonPluginConfiguration.MapShape.values())
			.map(shape -> new JacksonPluginConfiguration(shape));
	}
}
