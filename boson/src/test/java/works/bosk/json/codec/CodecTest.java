package works.bosk.json.codec;

import java.io.IOException;
import java.io.StringWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import works.bosk.json.TestUtils.OneOfEach;
import works.bosk.json.codec.io.CharArrayJsonReader;
import works.bosk.json.mapping.TypeMap;
import works.bosk.json.mapping.spec.JsonValueSpec;
import works.bosk.json.types.DataType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static works.bosk.json.TestUtils.ONE_OF_EACH;
import static works.bosk.json.TestUtils.expectedOneOfEach;
import static works.bosk.json.codec.compiler.SpecCompilerTest.testTypeMap;

class CodecTest {
	TypeMap typeMap;
	private JsonValueSpec spec;

	@BeforeEach
	void init() throws NoSuchMethodException, IllegalAccessException {
		DataType type = DataType.of(OneOfEach.class);
		typeMap = testTypeMap(type);
		spec = typeMap.get(type);
	}

	@Test
	void testParser() throws IOException {
		LOGGER.debug("Spec: {}", spec);
		Codec codec = CodecBuilder.of(typeMap).build();
		assertEquals(expectedOneOfEach(), codec.parserFor(spec).parse(CharArrayJsonReader.forString(ONE_OF_EACH)));
	}

	@Test
	void roundTrip() throws IOException {
		StringWriter sw = new StringWriter();

		Codec codec = CodecBuilder.of(typeMap).build();
		codec.generatorFor(spec).generate(sw, expectedOneOfEach());
		var actual = codec.parserFor(spec).parse(CharArrayJsonReader.forString(sw.toString()));
		assertEquals(expectedOneOfEach(), actual);
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(CodecTest.class);
}
