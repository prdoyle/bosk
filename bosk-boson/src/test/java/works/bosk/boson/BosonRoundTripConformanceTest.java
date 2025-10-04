package works.bosk.boson;

import works.bosk.AbstractRoundTripTest;
import works.bosk.Reference;
import works.bosk.json.codec.Codec;
import works.bosk.json.codec.CodecBuilder;
import works.bosk.json.mapping.TypeMap;
import works.bosk.json.mapping.TypeScanner;
import works.bosk.json.types.DataType;
import works.bosk.testing.drivers.DriverConformanceTest;

class BosonRoundTripConformanceTest extends DriverConformanceTest {
	BosonRoundTripConformanceTest() {
		driverFactory = (b,d) -> {
			TypeMap typeMap = new TypeScanner(TypeMap.Settings.DEFAULT)
				.addLast(new BosonSerializer().bundleFor(b))
				.build();
			var rootType = DataType.of(b.rootReference().targetType());
			Codec codec = CodecBuilder.of(typeMap).build();
			return new AbstractRoundTripTest.PreprocessingDriver(d) {
				@Override
				protected <T> T preprocess(Reference<T> reference, T newValue) {
					return null;
				}
			};
		};
	}

}
