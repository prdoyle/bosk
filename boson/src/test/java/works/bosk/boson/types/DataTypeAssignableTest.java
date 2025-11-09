package works.bosk.boson.types;

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
	<T> void consistentBindings() {
		var mapTT = DataType.of(new TypeReference<Map<T, T>>() { });
		var mapSS = DataType.of(new TypeReference<Map<String, String>>() { });
		var mapSI = DataType.of(new TypeReference<Map<String, Integer>>() { });

		assertTrue(mapTT
			.isAssignableFrom(mapSS));
		assertTrue(mapTT
			.isAssignableFromTypeArgument(mapSS));

		assertFalse(mapTT
			.isAssignableFrom(mapSI));
		assertFalse(mapTT
			.isAssignableFromTypeArgument(mapSI));
	}

	@Test
	<T> void consistentBindingsInUpperBounds() {
		var mapExtendsT = DataType.of(new TypeReference<Map<T, ? extends T>>() { });

		var mapCO = DataType.of(new TypeReference<Map<CharSequence, Object>>() { });
		assertFalse(mapExtendsT
			.isAssignableFrom(mapCO));
		assertFalse(mapExtendsT
			.isAssignableFromTypeArgument(mapCO));

		var mapCC = DataType.of(new TypeReference<Map<CharSequence, CharSequence>>() { });
		assertTrue(mapExtendsT
			.isAssignableFrom(mapCC));
		assertTrue(mapExtendsT
			.isAssignableFromTypeArgument(mapCC));

		var mapCS = DataType.of(new TypeReference<Map<CharSequence, String>>() { });
		assertTrue(mapExtendsT
			.isAssignableFrom(mapCS));
		assertTrue(mapExtendsT
			.isAssignableFromTypeArgument(mapCS));

		var mapCI = DataType.of(new TypeReference<Map<CharSequence, Integer>>() { });
		assertFalse(mapExtendsT
			.isAssignableFrom(mapCI));
		assertFalse(mapExtendsT
			.isAssignableFromTypeArgument(mapCI));
	}

	@Test
	<T> void consistentBindingsInLowerBounds() {
		var mapSuperT = DataType.of(new TypeReference<Map<T, ? super T>>() { });

		var mapCO = DataType.of(new TypeReference<Map<CharSequence, Object>>() { });
		assertTrue(mapSuperT
			.isAssignableFrom(mapCO));
		assertTrue(mapSuperT
			.isAssignableFromTypeArgument(mapCO));

		var mapCC = DataType.of(new TypeReference<Map<CharSequence, CharSequence>>() { });
		assertTrue(mapSuperT
			.isAssignableFrom(mapCC));
		assertTrue(mapSuperT
			.isAssignableFromTypeArgument(mapCC));

		var mapCS = DataType.of(new TypeReference<Map<CharSequence, String>>() { });
		assertFalse(mapSuperT
			.isAssignableFrom(mapCS));
		assertFalse(mapSuperT
			.isAssignableFromTypeArgument(mapCS));

		var mapCI = DataType.of(new TypeReference<Map<CharSequence, Integer>>() { });
		assertFalse(mapSuperT
			.isAssignableFrom(mapCI));
		assertFalse(mapSuperT
			.isAssignableFromTypeArgument(mapCI));
	}

	@Test
	<T, L extends List<?>> void erased() {
		var listErased = DataType.of(List.class);
		var listString = DataType.of(new TypeReference<List<String>>() { });

		assertTrue(listErased
			.isAssignableFrom(listString));
		assertTrue(listErased
			.isAssignableFromTypeArgument(listString));

		assertTrue(listString
			.isAssignableFrom(listErased));
		assertTrue(listString
			.isAssignableFromTypeArgument(listErased));

		var listT = DataType.of(new TypeReference<T>() { });

		assertFalse(listErased
			.isAssignableFrom(listT));
		assertFalse(listErased
			.isAssignableFromTypeArgument(listT));

		var listL = DataType.of(new TypeReference<L>() { });

		assertTrue(listErased
			.isAssignableFrom(listL));
		assertFalse(listErased
			.isAssignableFromTypeArgument(listL));
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

	@Test
	void rawTypeMatchesParameterizedType() {
		assertTrue(DataType.of(List.class)
			.isAssignableFrom(DataType.of(new TypeReference<List<String>>() { })));
		assertTrue(DataType.of(List.class)
			.isAssignableFromTypeArgument(DataType.of(new TypeReference<List<String>>() { })));
		assertTrue(DataType.of(new TypeReference<List<String>>() { })
			.isAssignableFrom(DataType.of(List.class)));
		assertTrue(DataType.of(new TypeReference<List<String>>() { })
			.isAssignableFromTypeArgument(DataType.of(List.class)));
		assertTrue(DataType.of(Iterable.class)
			.isAssignableFrom(DataType.of(new TypeReference<List<String>>() { })));
	}

	@Test
	void upperBoundedWildcardComparedToUpperBoundedWildcard() {
		assertTrue(DataType.of(new TypeReference<List<? extends CharSequence>>() { })
			.isAssignableFrom(DataType.of(new TypeReference<List<? extends String>>() { })));
		assertTrue(DataType.of(new TypeReference<List<? extends CharSequence>>() { })
			.isAssignableFromTypeArgument(DataType.of(new TypeReference<List<? extends String>>() { })));
	}

	@Test
	void lowerBoundedWildcardComparedToLowerBoundedWildcard() {
		assertTrue(DataType.of(new TypeReference<List<? super String>>() { })
			.isAssignableFrom(DataType.of(new TypeReference<List<? super CharSequence>>() { })));
		assertTrue(DataType.of(new TypeReference<List<? super String>>() { })
			.isAssignableFromTypeArgument(DataType.of(new TypeReference<List<? super CharSequence>>() { })));
	}

	@Test
	void primitiveArrayMatchesSupertypes() {
		assertTrue(DataType.of(Object.class).isAssignableFrom(DataType.of(int[].class)));
		assertTrue(DataType.of(Cloneable.class).isAssignableFrom(DataType.of(int[].class)));
		assertTrue(DataType.of(Serializable.class).isAssignableFrom(DataType.of(int[].class)));
	}

	@Test
	void multiDimensionalArrayMatchesSupertypes() {
		assertTrue(DataType.of(Object.class).isAssignableFrom(DataType.of(String[][].class)));
		assertTrue(DataType.of(Cloneable.class).isAssignableFrom(DataType.of(String[][].class)));
		assertTrue(DataType.of(Serializable.class).isAssignableFrom(DataType.of(String[][].class)));
	}

	@Test
	void arrayCovariance() {
		assertTrue(DataType.of(Object[].class).isAssignableFrom(DataType.of(String[].class)));
		assertFalse(DataType.of(String[].class).isAssignableFrom(DataType.of(Object[].class)));

		assertFalse(DataType.of(Object[].class).isAssignableFromTypeArgument(DataType.of(String[].class)));
		assertFalse(DataType.of(String[].class).isAssignableFromTypeArgument(DataType.of(Object[].class)));
	}

	@Test
	<T> void arrayVsNonArray() {
		assertFalse(DataType.of(String[].class)
			.isAssignableFrom(DataType.of(Object.class)));
		assertFalse(DataType.of(String[].class)
			.isAssignableFromTypeArgument(DataType.of(Object.class)));

		assertFalse(DataType.of(new TypeReference<T[]>(){})
			.isAssignableFrom(DataType.of(Object.class)));
		assertFalse(DataType.of(new TypeReference<T[]>(){})
			.isAssignableFromTypeArgument(DataType.of(Object.class)));
	}

	@Test
	void arrayIsNotAssignableToUnrelatedInterface() {
		assertFalse(DataType.of(List.class).isAssignableFrom(DataType.of(String[].class)));
	}

	@Test
	void parameterTypeMappingThroughInheritance() {
		assertTrue(DataType.of(new TypeReference<List<String>>() { })
			.isAssignableFrom(DataType.of(new TypeReference<ArrayList<String>>() { })));
		assertFalse(DataType.of(new TypeReference<List<String>>() { })
			.isAssignableFromTypeArgument(DataType.of(new TypeReference<ArrayList<String>>() { })));
	}

	@Test
	<T> void arrayOfUnboundedVariable() {
	    var arrayOfT = DataType.of(new TypeReference<T[]>() { });
		DataType arrayOfString = DataType.of(String[].class);
		assertTrue(arrayOfT.isAssignableFrom(arrayOfString));
	    assertTrue(arrayOfT.isAssignableFromTypeArgument(arrayOfString));
	}

	@Test
	<T extends CharSequence> void arrayOfBoundedVariable() {
	    var arrayOfT = DataType.of(new TypeReference<T[]>() { });

		DataType arrayOfString = DataType.of(String[].class);
	    assertTrue(arrayOfT.isAssignableFrom(arrayOfString));
	    assertTrue(arrayOfT.isAssignableFromTypeArgument(arrayOfString));

		DataType arrayOfObject = DataType.of(Object[].class);
	    assertFalse(arrayOfT.isAssignableFrom(arrayOfObject));
	    assertFalse(arrayOfT.isAssignableFromTypeArgument(arrayOfObject));
	}

	@Test
	<X extends CharSequence & Constable> void arrayOfMultiBoundedVariable() {
		DataType arrayOfX = DataType.of(new TypeReference<X[]>() { });
		assertTrue(arrayOfX
			.isAssignableFrom(DataType.of(String[].class)));
		assertTrue(arrayOfX
			.isAssignableFromTypeArgument(DataType.of(String[].class)));

		// Integer is Constable but not CharSequence
		assertFalse(arrayOfX
			.isAssignableFrom(DataType.of(Integer[].class)));
		assertFalse(arrayOfX
			.isAssignableFromTypeArgument(DataType.of(Integer[].class)));
	}

	@Test
	void arrayOfUnboundedWildcard() {
		var arrayOfWildcard = new UnknownArrayType(new UnboundedWildcardType());
		var arrayofString = DataType.of(new TypeReference<String[]>() { });
		assertTrue(arrayOfWildcard
			.isAssignableFrom(arrayofString));
		assertTrue(arrayOfWildcard
			.isAssignableFromTypeArgument(arrayofString));
	}

	@Test
	void arrayOfUpperBoundedWildcard() {
		var arrayOfWildcard = new UnknownArrayType(
			new UpperBoundedWildcardType(DataType.of(CharSequence.class))
		);

		var arrayOfObject = DataType.of(new TypeReference<Object[]>() { });
		assertFalse(arrayOfWildcard
			.isAssignableFrom(arrayOfObject));
		assertFalse(arrayOfWildcard
			.isAssignableFromTypeArgument(arrayOfObject));

		var arrayOfCharSequence = DataType.of(new TypeReference<CharSequence[]>() { });
		assertTrue(arrayOfWildcard
			.isAssignableFrom(arrayOfCharSequence));
		assertTrue(arrayOfWildcard
			.isAssignableFromTypeArgument(arrayOfCharSequence));

		var arrayofString = DataType.of(new TypeReference<String[]>() { });
		assertTrue(arrayOfWildcard
			.isAssignableFrom(arrayofString));
		assertTrue(arrayOfWildcard
			.isAssignableFromTypeArgument(arrayofString));
	}

	@Test
	void arrayOfLowerBoundedWildcard() {
		var arrayOfWildcard = new UnknownArrayType(
			new LowerBoundedWildcardType(DataType.of(CharSequence.class))
		);

		var arrayOfObject = DataType.of(new TypeReference<Object[]>() { });
		assertTrue(arrayOfWildcard
			.isAssignableFrom(arrayOfObject));
		assertTrue(arrayOfWildcard
			.isAssignableFromTypeArgument(arrayOfObject));

		var arrayOfCharSequence = DataType.of(new TypeReference<CharSequence[]>() { });
		assertTrue(arrayOfWildcard
			.isAssignableFrom(arrayOfCharSequence));
		assertTrue(arrayOfWildcard
			.isAssignableFromTypeArgument(arrayOfCharSequence));

		var arrayofString = DataType.of(new TypeReference<String[]>() { });
		assertFalse(arrayOfWildcard
			.isAssignableFrom(arrayofString));
		assertFalse(arrayOfWildcard
			.isAssignableFromTypeArgument(arrayofString));
	}

}
