package works.bosk.boson.types;

import java.lang.constant.Constable;
import java.util.List;
import org.junit.jupiter.api.Test;

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
		assertFalse(DataType.of(int.class).isAssignableFrom(Integer.class));
		assertFalse(DataType.of(int.class).isAssignableFrom(DataType.of(Integer.class)));
		assertFalse(DataType.of(Integer.class).isAssignableFrom(int.class));
		assertFalse(DataType.of(Integer.class).isAssignableFrom(DataType.of(int.class)));
	}

	@Test
	void concreteGenerics() {
		assertTrue(DataType.of(new TypeReference<List<String>>() { })
			.isAssignableFrom(DataType.of(new TypeReference<List<String>>() { })));
		assertTrue(DataType.of(new TypeReference<Comparable<String>>() { })
			.isAssignableFrom(DataType.of(new TypeReference<String>() { })));
	}

	@Test
	void genericSubtype() {
		assertFalse(DataType.of(new TypeReference<List<CharSequence>>() { })
			.isAssignableFrom(new TypeReference<List<String>>() { }),
			"Generics are not covariant");
		assertFalse(DataType.of(new TypeReference<List<String>>() { })
			.isAssignableFrom(new TypeReference<List<CharSequence>>() { }),
			"Generics are not contravariant");
		assertTrue(DataType.of(new TypeReference<List<?>>() { })
			.isAssignableFrom(new TypeReference<List<String>>() { }),
			"Unbounded wildcard matches any type");
		assertTrue(DataType.of(new TypeReference<List<? extends CharSequence>>() { })
			.isAssignableFrom(new TypeReference<List<String>>() { }),
			"Upper-bounded CharSequence matches String");
		assertTrue(DataType.of(new TypeReference<List<? super String>>() { })
			.isAssignableFrom(new TypeReference<List<CharSequence>>() { }),
			"Lower-bounded String matches CharSequence");
	}

	@Test
	<V, C extends CharSequence, S extends String, X extends CharSequence & Constable> void typeVariables() {
		assertTrue(new TypeVariable("A")
			.isAssignableFrom(new TypeVariable("B")),
			"Unbounded type variable matches type variable");
		assertTrue(new TypeVariable("A")
			.isAssignableFrom(new TypeVariable("B", String.class)),
			"Unbounded type variable matches type variable with bound");
		assertFalse(new TypeVariable("A", String.class)
				.isAssignableFrom(new TypeVariable("B")),
			"Bounded type variable does not match unbounded type variable");
		assertTrue(new TypeVariable("A", CharSequence.class)
				.isAssignableFrom(new TypeVariable("B", String.class)),
			"Bounded type variable matches type variable with subtype bound");
		assertFalse(new TypeVariable("A", String.class)
				.isAssignableFrom(new TypeVariable("B", CharSequence.class)),
			"Bounded type variable does not match type variable with supertype bound");
		assertTrue(DataType.of(new TypeReference<X>(){})
			.isAssignableFrom(DataType.of(String.class)),
			"Type variable with multiple bounds matches subtype");
		assertFalse(DataType.of(new TypeReference<X>(){})
			.isAssignableFrom(DataType.of(Integer.class)),
			"Type variable with multiple bounds does not match type satisfying just one bound");
		assertTrue(DataType.STRING
			.isAssignableFrom(DataType.of(new TypeReference<S>() { })),
			"String matches type variable bounded to String");
		assertFalse(DataType.STRING
				.isAssignableFrom(DataType.of(new TypeReference<C>() { })),
			"String does not match type variable bounded only to supertype");
		assertTrue(DataType.OBJECT
				.isAssignableFrom(DataType.of(new TypeReference<X>() { })),
			"Object matches type variable bounded to multiple subtypes");
		assertFalse(DataType.STRING
				.isAssignableFrom(DataType.of(new TypeReference<X>() { })),
			"String does not match type variable bounded only to multiple supertypes");
		assertTrue(DataType.of(CharSequence.class)
			.isAssignableFrom(DataType.of(new TypeReference<X>() { })),
			"CharSequence matches type variable if just one bound matches");
		assertTrue(DataType.of(new TypeReference<List<V>>() { })
			.isAssignableFrom(new TypeReference<List<String>>() { }),
			"Unbounded type variable matches anything");
		assertTrue(DataType.of(new TypeReference<List<C>>() { })
			.isAssignableFrom(new TypeReference<List<CharSequence>>() { }),
			"Bounded type variable matches bound");
		assertTrue(DataType.of(new TypeReference<List<C>>() { })
			.isAssignableFrom(new TypeReference<List<String>>() { }),
			"Bounded type variable matches subtype");
		assertFalse(DataType.of(new TypeReference<List<C>>() { })
			.isAssignableFrom(new TypeReference<List<Integer>>() { }),
			"Bounded type variable does not match unrelated type");
		assertFalse(DataType.of(new TypeReference<List<S>>() { })
			.isAssignableFrom(new TypeReference<List<CharSequence>>() { }),
			"Bounded type variable does not match supertype");
	}

}
