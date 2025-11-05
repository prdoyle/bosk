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
		assertFalse(DataType.of(Number.class).isAssignableFromTypeArgument(DataType.of(Integer.class)), "Type argument assignability is not covariant");
		assertFalse(DataType.of(Integer.class).isAssignableFrom(DataType.of(Number.class)));
		assertFalse(DataType.of(Integer.class).isAssignableFromTypeArgument(DataType.of(Number.class)));
	}

	@Test
	void boxing() {
		assertFalse(DataType.of(int.class).isAssignableFrom(Integer.class));
		assertFalse(DataType.of(int.class).isAssignableFromTypeArgument(DataType.of(Integer.class)));
		assertFalse(DataType.of(int.class).isAssignableFrom(DataType.of(Integer.class)));
		assertFalse(DataType.of(int.class).isAssignableFromTypeArgument(DataType.of(Integer.class)));
		assertFalse(DataType.of(Integer.class).isAssignableFrom(int.class));
		assertFalse(DataType.of(Integer.class).isAssignableFromTypeArgument(DataType.of(int.class)));
		assertFalse(DataType.of(Integer.class).isAssignableFrom(DataType.of(int.class)));
		assertFalse(DataType.of(Integer.class).isAssignableFromTypeArgument(DataType.of(int.class)));
	}

	@Test
	void genericTypeMatchesItself() {
		assertTrue(DataType.of(new TypeReference<List<String>>() { })
			.isAssignableFrom(DataType.of(new TypeReference<List<String>>() { })));
		assertTrue(DataType.of(new TypeReference<List<String>>() { })
			.isAssignableFromTypeArgument(DataType.of(new TypeReference<List<String>>() { })));
	}

	@Test
	void genericTypeMatchesSupertype() {
		assertTrue(DataType.of(new TypeReference<Comparable<String>>() { })
			.isAssignableFrom(DataType.of(new TypeReference<String>() { })));
		assertFalse(DataType.of(new TypeReference<Comparable<String>>() { })
			.isAssignableFromTypeArgument(DataType.of(new TypeReference<String>() { })),
			"Type argument assignability is not covariant");
	}

	@Test
	void genericsAreNotCovariant() {
		assertFalse(DataType.of(new TypeReference<List<CharSequence>>() { })
			.isAssignableFrom(new TypeReference<List<String>>() { }));
		assertFalse(DataType.of(new TypeReference<List<CharSequence>>() { })
			.isAssignableFromTypeArgument(DataType.of(new TypeReference<List<String>>() { })));
	}

	@Test
	void genericsAreNotContravariant() {
		assertFalse(DataType.of(new TypeReference<List<String>>() { })
			.isAssignableFrom(new TypeReference<List<CharSequence>>() { }));
		assertFalse(DataType.of(new TypeReference<List<String>>() { })
			.isAssignableFromTypeArgument(DataType.of(new TypeReference<List<CharSequence>>() { })));
	}

	@Test
	void unboundedWildcardMatchesAnyType() {
		assertTrue(DataType.of(new TypeReference<List<?>>() { })
			.isAssignableFrom(new TypeReference<List<String>>() { }));
		assertTrue(DataType.of(new TypeReference<List<?>>() { })
			.isAssignableFromTypeArgument(DataType.of(new TypeReference<List<String>>() { })));
	}

	@Test
	void upperBoundedCharSequenceMatchesString() {
		assertTrue(DataType.of(new TypeReference<List<? extends CharSequence>>() { })
			.isAssignableFrom(new TypeReference<List<String>>() { }));
		assertTrue(DataType.of(new TypeReference<List<? extends CharSequence>>() { })
			.isAssignableFromTypeArgument(DataType.of(new TypeReference<List<String>>() { })));
	}

	@Test
	void lowerBoundedStringMatchesCharSequence() {
		assertTrue(DataType.of(new TypeReference<List<? super String>>() { })
			.isAssignableFrom(new TypeReference<List<CharSequence>>() { }));
		assertTrue(DataType.of(new TypeReference<List<? super String>>() { })
			.isAssignableFromTypeArgument(DataType.of(new TypeReference<List<CharSequence>>() { })));
	}

	@Test
	void unboundedTypeVariableMatchesTypeVariable() {
		assertTrue(new TypeVariable("A")
			.isAssignableFrom(new TypeVariable("B")));
		assertTrue(new TypeVariable("A")
			.isAssignableFromTypeArgument(new TypeVariable("B")));
	}

	@Test
	void unboundedTypeVariableMatchesTypeVariableWithBound() {
		assertTrue(new TypeVariable("A")
			.isAssignableFrom(new TypeVariable("B", String.class)));
		assertTrue(new TypeVariable("A")
			.isAssignableFromTypeArgument(new TypeVariable("B", String.class)));
	}

	@Test
	void boundedTypeVariableDoesNotMatchUnboundedTypeVariable() {
		assertFalse(new TypeVariable("A", String.class)
			.isAssignableFrom(new TypeVariable("B")));
		assertFalse(new TypeVariable("A", String.class)
			.isAssignableFromTypeArgument(new TypeVariable("B")));
	}

	@Test
	void boundedTypeVariableMatchesTypeVariableWithSubtypeBound() {
		assertTrue(new TypeVariable("A", CharSequence.class)
			.isAssignableFrom(new TypeVariable("B", String.class)));
		assertTrue(new TypeVariable("A", CharSequence.class)
			.isAssignableFromTypeArgument(new TypeVariable("B", String.class)));
	}

	@Test
	void boundedTypeVariableDoesNotMatchTypeVariableWithSupertypeBound() {
		assertFalse(new TypeVariable("A", String.class)
			.isAssignableFrom(new TypeVariable("B", CharSequence.class)));
		assertFalse(new TypeVariable("A", String.class)
			.isAssignableFromTypeArgument(new TypeVariable("B", CharSequence.class)));
	}

	@Test
	<X extends CharSequence & Constable> void typeVariableWithMultipleBoundsMatchesSubtype() {
		assertTrue(DataType.of(new TypeReference<X>(){})
			.isAssignableFrom(DataType.of(String.class)));
		assertTrue(DataType.of(new TypeReference<X>(){})
			.isAssignableFromTypeArgument(DataType.of(String.class)));
	}

	@Test
	<X extends CharSequence & Constable> void typeVariableWithMultipleBoundsDoesNotMatchTypeSatisfyingJustOneBound() {
		assertFalse(DataType.of(new TypeReference<X>(){})
			.isAssignableFrom(DataType.of(Integer.class)));
		assertFalse(DataType.of(new TypeReference<X>(){})
			.isAssignableFromTypeArgument(DataType.of(Integer.class)));
	}

	@Test
	<S extends String> void stringMatchesTypeVariableBoundedToString() {
		assertTrue(DataType.STRING
			.isAssignableFrom(DataType.of(new TypeReference<S>() { })));
		assertFalse(DataType.STRING
			.isAssignableFromTypeArgument(DataType.of(new TypeReference<S>() { })),
			"Type argument assignability is not covariant");
	}

	@Test
	<C extends CharSequence> void stringDoesNotMatchTypeVariableBoundedOnlyToSupertype() {
		assertFalse(DataType.STRING
			.isAssignableFrom(DataType.of(new TypeReference<C>() { })));
		assertFalse(DataType.STRING
			.isAssignableFromTypeArgument(DataType.of(new TypeReference<C>() { })));
	}

	@Test
	<X extends CharSequence & Constable> void objectMatchesTypeVariableBoundedToMultipleSubtypes() {
		assertTrue(DataType.OBJECT
			.isAssignableFrom(DataType.of(new TypeReference<X>() { })));
		assertFalse(DataType.OBJECT
			.isAssignableFromTypeArgument(DataType.of(new TypeReference<X>() { })),
			"Type argument assignability is not covariant");
	}

	@Test
	<X extends CharSequence & Constable> void stringDoesNotMatchTypeVariableBoundedOnlyToMultipleSupertypes() {
		assertFalse(DataType.STRING
			.isAssignableFrom(DataType.of(new TypeReference<X>() { })));
		assertFalse(DataType.STRING
			.isAssignableFromTypeArgument(DataType.of(new TypeReference<X>() { })));
	}

	@Test
	<X extends CharSequence & Constable> void charSequenceMatchesTypeVariableIfJustOneBoundMatches() {
		assertTrue(DataType.of(CharSequence.class)
			.isAssignableFrom(DataType.of(new TypeReference<X>() { })));
		assertFalse(DataType.of(CharSequence.class)
			.isAssignableFromTypeArgument(DataType.of(new TypeReference<X>() { })),
			"Type argument assignability is not covariant");
	}

	@Test
	<V> void unboundedTypeVariableMatchesAnything() {
		assertTrue(DataType.of(new TypeReference<List<V>>() { })
			.isAssignableFrom(new TypeReference<List<String>>() { }));
		assertTrue(DataType.of(new TypeReference<List<V>>() { })
			.isAssignableFromTypeArgument(DataType.of(new TypeReference<List<String>>() { })));
	}

	@Test
	<C extends CharSequence> void boundedTypeVariableMatchesBound() {
		assertTrue(DataType.of(new TypeReference<List<C>>() { })
			.isAssignableFrom(new TypeReference<List<CharSequence>>() { }));
		assertTrue(DataType.of(new TypeReference<List<C>>() { })
			.isAssignableFromTypeArgument(DataType.of(new TypeReference<List<CharSequence>>() { })));
	}

	@Test
	<C extends CharSequence> void boundedTypeVariableMatchesSubtype() {
		assertTrue(DataType.of(new TypeReference<List<C>>() { })
			.isAssignableFrom(new TypeReference<List<String>>() { }));
		assertTrue(DataType.of(new TypeReference<List<C>>() { })
			.isAssignableFromTypeArgument(DataType.of(new TypeReference<List<String>>() { })));
	}

	@Test
	<C extends CharSequence> void boundedTypeVariableDoesNotMatchUnrelatedType() {
		assertFalse(DataType.of(new TypeReference<List<C>>() { })
			.isAssignableFrom(new TypeReference<List<Integer>>() { }));
		assertFalse(DataType.of(new TypeReference<List<C>>() { })
			.isAssignableFromTypeArgument(DataType.of(new TypeReference<List<Integer>>() { })));
	}

	@Test
	<S extends String> void boundedTypeVariableDoesNotMatchSupertype() {
		assertFalse(DataType.of(new TypeReference<List<S>>() { })
			.isAssignableFrom(new TypeReference<List<CharSequence>>() { }));
		assertFalse(DataType.of(new TypeReference<List<S>>() { })
			.isAssignableFromTypeArgument(DataType.of(new TypeReference<List<CharSequence>>() { })));
	}

}
