package works.bosk.json.types;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import works.bosk.json.types.DataType.KnownType;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataTypeAssignableTest {

	@Test
	void simpleSubtype() {
		assertTrue(DataType.of(Number.class).isAssignableFrom(DataType.of(Integer.class)));
		assertFalse(DataType.of(Integer.class).isAssignableFrom(DataType.of(Number.class)));
	}

	@Test
	void boxing() {
		assertFalse(DataType.of(int.class).isAssignableFrom(DataType.of(Integer.class)));
		assertFalse(DataType.of(Integer.class).isAssignableFrom(DataType.of(int.class)));
	}

	@Test
	void concreteGenerics() {
		assertTrue(DataType.of(new TypeReference<List<String>>() { })
			.isAssignableFrom((KnownType) DataType.of(new TypeReference<List<String>>() { })));
		assertTrue(DataType.of(new TypeReference<Comparable<String>>() { })
			.isAssignableFrom((KnownType) DataType.of(new TypeReference<String>() { })));
	}

	@Test
	void genericSubtype() {
		assertFalse(DataType.of(new TypeReference<List<CharSequence>>() { })
			.isAssignableFrom((KnownType) DataType.of(new TypeReference<List<String>>() { })),
			"Generics are not covariant");
		assertFalse(DataType.of(new TypeReference<List<String>>() { })
				.isAssignableFrom((KnownType) DataType.of(new TypeReference<List<CharSequence>>() { })),
			"Generics are not contravariant");
		assertTrue(DataType.of(new TypeReference<List<?>>() { })
			.isAssignableFrom((KnownType) DataType.of(new TypeReference<List<String>>() { })),
			"Unbounded wildcard matches any type");
		assertTrue(DataType.of(new TypeReference<List<? extends CharSequence>>() { })
			.isAssignableFrom((KnownType) DataType.of(new TypeReference<List<String>>() { })),
			"Upper-bounded CharSequence matches String");
		assertTrue(DataType.of(new TypeReference<List<? super String>>() { })
			.isAssignableFrom((KnownType) DataType.of(new TypeReference<List<CharSequence>>() { })),
			"Lower-bounded String matches CharSequence");
	}

	@Test
	<T1, T2, T1Sub extends T1> void typeVariables() {
		assertTrue(DataType.of(new TypeReference<Map<T1,T2>>() { })
				.isAssignableFrom((KnownType) DataType.of(new TypeReference<Map<String,Integer>>() { })),
			"Unbounded type variables can match any type");
		assertFalse(DataType.of(new TypeReference<Map<T1,T1>>() { })
				.isAssignableFrom((KnownType) DataType.of(new TypeReference<Map<String,Integer>>() { })),
			"Type variables must match each other");
		assertTrue(DataType.of(new TypeReference<Map<T1,T1Sub>>() { })
				.isAssignableFrom((KnownType) DataType.of(new TypeReference<Map<CharSequence,String>>() { })),
			"Type variable with upper bound can match subtype");
		assertFalse(DataType.of(new TypeReference<Map<T1Sub,T1>>() { })
				.isAssignableFrom((KnownType) DataType.of(new TypeReference<Map<CharSequence,String>>() { })),
			"Type variable with subtype bound cannot match supertype");
	}
}
