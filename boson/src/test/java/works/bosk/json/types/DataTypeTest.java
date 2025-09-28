package works.bosk.json.types;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import works.bosk.json.types.DataType.ArrayType;
import works.bosk.json.types.DataType.BoundType;
import works.bosk.json.types.DataType.TypeVariable;
import works.bosk.json.types.DataType.UnknownArrayType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static works.bosk.json.types.DataType.PrimitiveType.BOOLEAN;
import static works.bosk.json.types.DataType.PrimitiveType.BYTE;
import static works.bosk.json.types.DataType.PrimitiveType.CHAR;
import static works.bosk.json.types.DataType.PrimitiveType.DOUBLE;
import static works.bosk.json.types.DataType.PrimitiveType.FLOAT;
import static works.bosk.json.types.DataType.PrimitiveType.INT;
import static works.bosk.json.types.DataType.PrimitiveType.LONG;
import static works.bosk.json.types.DataType.PrimitiveType.SHORT;
import static works.bosk.json.types.DataType.WILDCARD;

class DataTypeTest {

	@Test
	void primitives() {
		assertEquals(BOOLEAN, DataType.of(boolean.class));
		assertEquals(BYTE, DataType.of(byte.class));
		assertEquals(SHORT, DataType.of(short.class));
		assertEquals(INT, DataType.of(int.class));
		assertEquals(LONG, DataType.of(long.class));
		assertEquals(FLOAT, DataType.of(float.class));
		assertEquals(DOUBLE, DataType.of(double.class));
		assertEquals(CHAR, DataType.of(char.class));
	}

	@Test
	<T> void arrays() {
		assertEquals(new ArrayType(INT), DataType.of(int[].class));
		assertEquals(new ArrayType(DataType.of(String.class)), DataType.of(String[].class));
		assertEquals(new ArrayType(new ArrayType(INT)), DataType.of(int[][].class));
		assertEquals(new UnknownArrayType(new TypeVariable("T")), DataType.of(new TypeReference<T[]>(){}));
	}

	@Test
	<T> void parameterized() {
		DataType listType = DataType.of(new TypeReference<
			List<String>
			>() {});
		assertEquals(
			new BoundType(List.class, List.of(DataType.of(String.class))),
			listType
		);

		DataType genericListType = DataType.of(new TypeReference<
			List<T>
			>() {});
		assertEquals(
			new BoundType(List.class, List.of(new TypeVariable("T"))),
			genericListType
		);

		DataType mapType = DataType.of(new TypeReference<
			Map<String, List<Integer>>
			>() {});
		assertEquals(
			new BoundType(Map.class, List.of(
				DataType.of(String.class),
				new BoundType(List.class, List.of(DataType.of(Integer.class)))
			)),
			mapType
		);
	}

	@Test
	void typeVariables() {
		class Generic<T extends Number, U extends Comparable<U>> {
		}
		record Holder<V extends Number, W extends Comparable<W>>(
			Generic<V, W> generic
		) { }
		var expectedType = new BoundType(
			Generic.class,
			List.of(
				new TypeVariable("V"),
				new TypeVariable("W")
			)
		);
		assertEquals(expectedType, DataType.of(Holder.class.getRecordComponents()[0].getGenericType()));
	}

	@Test
	void wildcards() {
		var upperBounded = new TypeReference<List<? extends Number>>() {
		};
		var expectedUpperBounded = new BoundType(
			List.class,
			List.of(WILDCARD)
		);
		assertEquals(expectedUpperBounded, DataType.of(upperBounded));

		var lowerBounded = new TypeReference<List<? super Integer>>() {
		};
		var expectedLowerBounded = new BoundType(
			List.class,
			List.of(WILDCARD)
		);
		assertEquals(expectedLowerBounded, DataType.of(lowerBounded));
	}

	@Test
	void unsupportedType() {
		var bogusType = new java.lang.reflect.Type() { };
		assertThrows(IllegalArgumentException.class, () -> DataType.of(bogusType));
	}

}
