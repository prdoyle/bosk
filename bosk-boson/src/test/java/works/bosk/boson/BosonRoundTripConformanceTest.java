package works.bosk.boson;

import java.io.IOException;
import java.io.StringWriter;
import works.bosk.AbstractRoundTripTest;
import works.bosk.Reference;
import works.bosk.json.codec.CharArrayReader;
import works.bosk.json.codec.Codec;
import works.bosk.json.codec.CodecBuilder;
import works.bosk.json.codec.Generator;
import works.bosk.json.codec.Parser;
import works.bosk.json.mapping.TypeMap;
import works.bosk.json.mapping.TypeScanner;
import works.bosk.json.types.DataType;
import works.bosk.testing.drivers.DriverConformanceTest;

class BosonRoundTripConformanceTest extends DriverConformanceTest {
	BosonRoundTripConformanceTest() {
		driverFactory = (b,d) -> {
			var rootType = DataType.of(b.rootReference().targetType());
			TypeMap typeMap = new TypeScanner(TypeMap.Settings.DEFAULT)
				.addLast(new BosonSerializer().bundleFor(b))
				.scan(rootType)
				.build();
			Codec codec = CodecBuilder.of(typeMap).build();
			return new AbstractRoundTripTest.PreprocessingDriver(d) {
				@Override
				protected <T> T preprocess(Reference<T> reference, T newValue) {
					Generator generator = codec.generatorFor(typeMap.get(rootType));
					var writer = new StringWriter();
					generator.generate(writer, newValue);
					Parser parser = codec.parserFor(typeMap.get(rootType));
					try {
						Object parsed = parser.parse(new CharArrayReader(writer.toString()));
						return reference.targetClass().cast(parsed);
					} catch (IOException e) {
						throw new AssertionError("Unexpected exception", e);
					}
				}
			};
		};
	}

}
