package works.bosk.json.mapping;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import works.bosk.json.codec.CharArrayReader;
import works.bosk.json.codec.Codec;
import works.bosk.json.codec.CodecBuilder;
import works.bosk.json.mapping.TypeScanner.Bundle;
import works.bosk.json.mapping.TypeScanner.Directive;
import works.bosk.json.mapping.spec.RepresentAsSpec;
import works.bosk.json.mapping.spec.TypeRefNode;
import works.bosk.json.types.DataType;
import works.bosk.json.types.TypeReference;
import works.bosk.json.types.WildcardType;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TypeScannerTest {
	TypeScanner scanner;

	@BeforeEach
	void setUp() {
		scanner = new TypeScanner(new TypeMap.Settings(false, false, false, false));
	}

	public record FloatAsString(String text) {}

	@Test
	void testSimpleDirective() throws IOException {
		scanner.addLast(new Bundle(List.of(
			new Directive(
				DataType.FLOAT,
				_ -> RepresentAsSpec.as(
					new TypeRefNode(DataType.known(FloatAsString.class)),
					DataType.FLOAT,
					(Float f) -> new FloatAsString(f.toString()),
					(FloatAsString s) -> Float.parseFloat(s.text())
				))
		)));
		var typeMap = scanner
			.scan(DataType.FLOAT)
			.scan(DataType.known(FloatAsString.class))
			.build();
		Codec codec = CodecBuilder.of(typeMap).build(typeMap.get(DataType.FLOAT));
		Object actual = codec.parse(new CharArrayReader(
			"""
			{ "text": "123.45" }
			"""));
		assertEquals(123.45f, actual);
	}

	public interface Overridden {
		int value();
	}

	public record OverriddenImpl(int value) implements Overridden {}

	@Test
	void testTypeBound() throws IOException {
		scanner.addLast(new Bundle(List.of(
			new Directive(
				WildcardType.extends_(Overridden.class),
				t -> RepresentAsSpec.asInt(
					t,
					Overridden::value,
					OverriddenImpl::new
				))
		)));
		DataType implType = DataType.of(new TypeReference<List<OverriddenImpl>>() { });
		var typeMap = scanner
			.scan(DataType.INT)
			.scan(implType) // This seems unfortunate
			.build();
		Codec codec = CodecBuilder.of(typeMap).build(typeMap.get(implType));
		Object actual = codec.parse(new CharArrayReader(
			"""
			[ 123, 456 ]
			"""));
		assertEquals(List.of(new OverriddenImpl(123), new OverriddenImpl(456)), actual);
	}
}
