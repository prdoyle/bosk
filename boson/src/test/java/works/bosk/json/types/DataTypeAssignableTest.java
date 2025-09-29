package works.bosk.json.types;

import java.io.Serializable;
import java.lang.constant.Constable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
	<T1, T2, T1Sub extends T1, Intersection extends Constable & Serializable>
	void typeVariables() {
		assertFalse(DataType.of(new TypeReference<T1>() { })
			.isAssignableFrom(DataType.of(new TypeReference<T2>() { })),
			"Unrelated type variables do not match");
		assertTrue(DataType.of(new TypeReference<Map<T1,T2>>() { })
				.isAssignableFrom(DataType.of(new TypeReference<Map<String,Integer>>() { })),
			"Unbounded type variables match any type");
		assertFalse(DataType.of(new TypeReference<Map<T1,T1>>() { })
				.isAssignableFrom(DataType.of(new TypeReference<Map<String,Integer>>() { })),
			"Type variables must match each other");
		assertTrue(DataType.of(new TypeReference<Map<T1,T1Sub>>() { })
				.isAssignableFrom(DataType.of(new TypeReference<Map<CharSequence,String>>() { })),
			"Type variable with upper bound match subtype");
		assertFalse(DataType.of(new TypeReference<Map<T1Sub,T1>>() { })
				.isAssignableFrom(DataType.of(new TypeReference<Map<CharSequence,String>>() { })),
			"Type variable with subtype bound does not match supertype");
		assertTrue(DataType.of(new TypeReference<Map<T1,? extends T1>>() { })
				.isAssignableFrom(DataType.of(new TypeReference<Map<CharSequence,String>>() { })),
			"Wildcard with upper bound matches subtype");
		assertTrue(DataType.of(new TypeReference<Map<T1,? super T1>>() { })
				.isAssignableFrom(DataType.of(new TypeReference<Map<String,CharSequence>>() { })),
			"Wildcard with lower bound matches supertype");
		assertFalse(DataType.of(new TypeReference<Intersection>() { })
				.isAssignableFrom(DataType.of(new TypeReference<ArrayList<String>>() { })),
			"Intersection type does not match class that is missing a supertype");
		assertTrue(DataType.of(new TypeReference<Intersection>() { })
			.isAssignableFrom(DataType.of(new TypeReference<Integer>() { })),
			"Intersection type matches appropriate class");

		// The rest of these ought to be true, but for our purposes,
		// we don't have any motivation to take the trouble to handle these.
		// In particular, we have no notion of a type variable's identity besides
		// its name, and that's not enough to avoid getting fooled.
		//
 		// You might ask: what if we change isAssignableFrom to take a KnownType?
		// That doesn't solve the problem, because List<T> is currently considered
		// a KnownType, and that just begs the question once you reach T.
		// We could solve this by deciding that a FullyKnownType must have
		// FullyKnownType parameters, but that's not the case right now.
		assertFalse(DataType.of(new TypeReference<T1>() { })
				.isAssignableFrom(DataType.of(new TypeReference<T1>() { })),
			"Nothing is assignable from an unknown type, even itself");
		assertFalse(DataType.of(new TypeReference<T1>() { })
				.isAssignableFrom(DataType.of(new TypeReference<T1Sub>() { })),
			"Subtype variable doesn't match because nothing is assignable from an unknown type");

	}
}
