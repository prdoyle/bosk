package works.bosk.boson;

import java.io.IOException;
import java.io.StringWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import works.bosk.AbstractRoundTripTest;
import works.bosk.Reference;
import works.bosk.json.codec.CharArrayReader;
import works.bosk.json.codec.Codec;
import works.bosk.json.codec.CodecBuilder;
import works.bosk.json.codec.Generator;
import works.bosk.json.codec.Parser;
import works.bosk.json.mapping.TypeMap;
import works.bosk.json.mapping.TypeScanner;
import works.bosk.json.mapping.spec.JsonValueSpec;
import works.bosk.json.types.DataType;
import works.bosk.testing.drivers.DriverConformanceTest;

class BosonRoundTripConformanceTest extends DriverConformanceTest {
	BosonRoundTripConformanceTest() {
		driverFactory = (b,d) -> {
			var rootType = DataType.of(b.rootReference().targetType());
			TypeScanner.Bundle bundle = new BosonSerializer().bundleFor(b);
			LOGGER.debug("Creating the real TypeScanner now for root type {}", rootType);
			TypeMap typeMap = new TypeScanner(TypeMap.Settings.DEFAULT.withCompiled(false))
				.addLast(bundle)
				.scan(rootType)
				.build();
			Codec codec = CodecBuilder.of(typeMap).build();
			return new AbstractRoundTripTest.PreprocessingDriver(d) {
				@Override
				protected <T> T preprocess(Reference<T> reference, T newValue) {
					JsonValueSpec targetSpec = typeMap.get(DataType.of(reference.targetType()));
					Generator generator = codec.generatorFor(targetSpec);
					var writer = new StringWriter();
					generator.generate(writer, newValue);
					Parser parser = codec.parserFor(targetSpec);
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

	private static final Logger LOGGER = LoggerFactory.getLogger(BosonRoundTripConformanceTest.class);
}
