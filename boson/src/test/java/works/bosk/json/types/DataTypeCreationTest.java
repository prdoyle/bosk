package works.bosk.json.types;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import works.bosk.json.types.DataType.ArrayType;
import works.bosk.json.types.DataType.BoundType;
import works.bosk.json.types.DataType.DeferredParameterOrBound;
import works.bosk.json.types.DataType.TypeVariable;
import works.bosk.json.types.DataType.UnknownArrayType;
import works.bosk.json.types.DataType.WildcardType;

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

class DataTypeCreationTest {

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
		assertEquals(new ArrayType(DataType.known(String.class)), DataType.of(String[].class));
		assertEquals(new ArrayType(new ArrayType(INT)), DataType.of(int[][].class));
		assertEquals(new UnknownArrayType(new TypeVariable("T")), DataType.of(new TypeReference<T[]>(){}));
	}

	@Test
	<T> void parameterized() {
		DataType listType = DataType.of(new TypeReference<
			List<String>
			>() {});
		assertEquals(
			new BoundType(List.class, List.of(new DeferredParameterOrBound(String.class))),
			listType
		);

		{
			var ref = new TypeReference<List<T>>() {
			};
			DataType genericListType = DataType.of(ref);
			assertEquals(
				new BoundType(List.class, List.of(new DeferredParameterOrBound(getActualTypeArguments(ref)[0]))),
				genericListType
			);
		}

		var ref = new TypeReference<Map<String, List<Integer>>>() { };
		DataType mapType = DataType.of(ref);
		assertEquals(
			new BoundType(Map.class, Stream.of(
				String.class,
				getActualTypeArguments(ref)[1]
			).map(DeferredParameterOrBound::new).toList()),
			mapType
		);
	}

	private static Type[] getActualTypeArguments(TypeReference<?> ref) {
		return ((ParameterizedType) ref.reflectionType()).getActualTypeArguments();
	}

	@Test
	void typeVariables() {
		class Generic<T extends Number, U extends Comparable<U>> {
		}
		record Holder<V extends Number, W extends Comparable<W>>(
			Generic<V, W> generic
		) { }
		ParameterizedType genericType = (ParameterizedType) Holder.class.getRecordComponents()[0].getGenericType();
		var actual = (BoundType)DataType.of(genericType);
		var actualV = (java.lang.reflect.TypeVariable<?>)genericType.getActualTypeArguments()[0];
		var actualW = (java.lang.reflect.TypeVariable<?>)genericType.getActualTypeArguments()[1];
		assertEquals(new TypeVariable("V", Stream.of(actualV.getBounds()).map(DeferredParameterOrBound::new).toList()),
			actual.typeArgument(0));
		assertEquals(new TypeVariable("W", Stream.of(actualW.getBounds()).map(DeferredParameterOrBound::new).toList()),
			actual.typeArgument(1));
	}

	@Test
	void wildcards() {
		var unbounded = (BoundType)DataType.of(new TypeReference<List<?>>() { });
		assertEquals(WildcardType.unbounded(), unbounded.typeArgument(0));

		var upperBounded = (BoundType)DataType.of(new TypeReference<List<? extends Number>>() { });
		assertEquals((WildcardType.extends_(Number.class)), upperBounded.typeArgument(0));

		var lowerBounded = (BoundType)DataType.of(new TypeReference<List<? super Integer>>() { });
		assertEquals((WildcardType.super_(Integer.class)), lowerBounded.typeArgument(0));
	}

	@Test
	void unsupportedType() {
		var bogusType = new java.lang.reflect.Type() { };
		assertThrows(IllegalArgumentException.class, () -> DataType.of(bogusType));
	}

}
