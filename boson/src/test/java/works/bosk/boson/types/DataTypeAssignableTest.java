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
		assertFalse(DataType.of(Number.class).isAssignableFromGenericParameter(DataType.of(Integer.class)), "Type argument assignability is not covariant");
		assertFalse(DataType.of(Integer.class).isAssignableFrom(DataType.of(Number.class)));
		assertFalse(DataType.of(Integer.class).isAssignableFromGenericParameter(DataType.of(Number.class)));
	}

	@Test
	void boxing() {
		// Boxing/unboxing are allowed by assignment conversion (JLS §5.1.7 / §5.2)
		// but Class.isAssignableFrom returns false for these, so who are we to argue?
		assertFalse(DataType.of(int.class).isAssignableFrom(Integer.class));
		assertFalse(DataType.of(int.class).isAssignableFrom(DataType.of(Integer.class)));
		assertFalse(DataType.of(Integer.class).isAssignableFrom(int.class));
		assertFalse(DataType.of(Integer.class).isAssignableFrom(DataType.of(int.class)));

		// JLS doesn't support primitives as generic type arguments, so the semantics here are up to us really
		assertFalse(DataType.of(int.class).isAssignableFromGenericParameter(DataType.of(Integer.class)));
		assertFalse(DataType.of(int.class).isAssignableFromGenericParameter(DataType.of(Integer.class)));
		assertFalse(DataType.of(Integer.class).isAssignableFromGenericParameter(DataType.of(int.class)));
		assertFalse(DataType.of(Integer.class).isAssignableFromGenericParameter(DataType.of(int.class)));
	}

	@Test
	void genericTypeMatchesItself() {
		assertTrue(DataType.of(new TypeReference<List<String>>() { })
			.isAssignableFrom(DataType.of(new TypeReference<List<String>>() { })));
		assertTrue(DataType.of(new TypeReference<List<String>>() { })
			.isAssignableFromGenericParameter(DataType.of(new TypeReference<List<String>>() { })));
	}

	@Test
	void genericTypeMatchesSupertype() {
		assertTrue(DataType.of(new TypeReference<Comparable<String>>() { })
			.isAssignableFrom(DataType.of(new TypeReference<String>() { })));
		assertFalse(DataType.of(new TypeReference<Comparable<String>>() {} )
				.isAssignableFromGenericParameter(DataType.of(new TypeReference<String>() { })),
			"Type argument assignability is not covariant");
	}

	@Test
	void genericsAreNotCovariant() {
		assertFalse(DataType.of(new TypeReference<List<CharSequence>>() { })
			.isAssignableFrom(new TypeReference<List<String>>() { }));
		assertFalse(DataType.of(new TypeReference<List<CharSequence>>() { })
			.isAssignableFromGenericParameter(DataType.of(new TypeReference<List<String>>() { })));
	}

	@Test
	void genericsAreNotContravariant() {
		assertFalse(DataType.of(new TypeReference<List<String>>() { })
			.isAssignableFrom(new TypeReference<List<CharSequence>>() { }));
		assertFalse(DataType.of(new TypeReference<List<String>>() { })
			.isAssignableFromGenericParameter(DataType.of(new TypeReference<List<CharSequence>>() { })));
	}

	@Test
	<T> void consistentBindings() {
		var mapTT = DataType.of(new TypeReference<Map<T, T>>() { });
		var mapSS = DataType.of(new TypeReference<Map<String, String>>() { });
		var mapSI = DataType.of(new TypeReference<Map<String, Integer>>() { });

		assertFalse(mapTT
			.isAssignableFrom(mapSS));
		assertTrue(mapTT
			.isAssignableFromGenericParameter(mapSS));

		assertFalse(mapTT
			.isAssignableFrom(mapSI));
		assertFalse(mapTT
			.isAssignableFromGenericParameter(mapSI));
	}

	@Test
	<T> void consistentBindingsInUpperBounds() {
		var mapExtendsT = DataType.of(new TypeReference<Map<T, ? extends T>>() { });

		var mapCO = DataType.of(new TypeReference<Map<CharSequence, Object>>() { });
		assertFalse(mapExtendsT
			.isAssignableFrom(mapCO));
		assertFalse(mapExtendsT
			.isAssignableFromGenericParameter(mapCO));

		var mapCC = DataType.of(new TypeReference<Map<CharSequence, CharSequence>>() { });
		assertFalse(mapExtendsT
			.isAssignableFrom(mapCC));
		assertTrue(mapExtendsT
			.isAssignableFromGenericParameter(mapCC));

		var mapCS = DataType.of(new TypeReference<Map<CharSequence, String>>() { });
		assertFalse(mapExtendsT
			.isAssignableFrom(mapCS));
		assertTrue(mapExtendsT
			.isAssignableFromGenericParameter(mapCS));

		var mapCI = DataType.of(new TypeReference<Map<CharSequence, Integer>>() { });
		assertFalse(mapExtendsT
			.isAssignableFrom(mapCI));
		assertFalse(mapExtendsT
			.isAssignableFromGenericParameter(mapCI));
	}

	@Test
	<T> void consistentBindingsInLowerBounds() {
		var mapSuperT = DataType.of(new TypeReference<Map<T, ? super T>>() { });

		var mapCO = DataType.of(new TypeReference<Map<CharSequence, Object>>() { });
		assertFalse(mapSuperT
			.isAssignableFrom(mapCO));
		assertTrue(mapSuperT
			.isAssignableFromGenericParameter(mapCO));

		var mapCC = DataType.of(new TypeReference<Map<CharSequence, CharSequence>>() { });
		assertFalse(mapSuperT
			.isAssignableFrom(mapCC));
		assertTrue(mapSuperT
			.isAssignableFromGenericParameter(mapCC));

		var mapCS = DataType.of(new TypeReference<Map<CharSequence, String>>() { });
		assertFalse(mapSuperT
			.isAssignableFrom(mapCS));
		assertFalse(mapSuperT
			.isAssignableFromGenericParameter(mapCS));

		var mapCI = DataType.of(new TypeReference<Map<CharSequence, Integer>>() { });
		assertFalse(mapSuperT
			.isAssignableFrom(mapCI));
		assertFalse(mapSuperT
			.isAssignableFromGenericParameter(mapCI));
	}

	@Test
	<T, L extends List<?>> void erased() {
		var listErased = DataType.of(List.class);
		var listString = DataType.of(new TypeReference<List<String>>() { });

		assertTrue(listErased
			.isAssignableFrom(listString));
		assertTrue(listErased
			.isAssignableFromGenericParameter(listString));

		assertTrue(listString
			.isAssignableFrom(listErased));
		assertTrue(listString
			.isAssignableFromGenericParameter(listErased));

		var listT = DataType.of(new TypeReference<T>() { });

		assertFalse(listErased
			.isAssignableFrom(listT));
		assertFalse(listErased
			.isAssignableFromGenericParameter(listT));

		var listL = DataType.of(new TypeReference<L>() { });

		assertTrue(listErased
			.isAssignableFrom(listL));
		assertFalse(listErased
			.isAssignableFromGenericParameter(listL));
	}

	@Test
	void unboundedWildcardMatchesAnyType() {
		assertTrue(DataType.of(new TypeReference<List<?>>() { })
			.isAssignableFrom(new TypeReference<List<String>>() { }));
		assertTrue(DataType.of(new TypeReference<List<?>>() { })
			.isAssignableFromGenericParameter(DataType.of(new TypeReference<List<String>>() { })));
	}

	@Test
	void upperBoundedCharSequenceMatchesString() {
		assertTrue(DataType.of(new TypeReference<List<? extends CharSequence>>() { })
			.isAssignableFrom(new TypeReference<List<String>>() { }));
		assertTrue(DataType.of(new TypeReference<List<? extends CharSequence>>() { })
			.isAssignableFromGenericParameter(DataType.of(new TypeReference<List<String>>() { })));
	}

	@Test
	void lowerBoundedStringMatchesCharSequence() {
		assertTrue(DataType.of(new TypeReference<List<? super String>>() { })
			.isAssignableFrom(new TypeReference<List<CharSequence>>() { }));
		assertTrue(DataType.of(new TypeReference<List<? super String>>() { })
			.isAssignableFromGenericParameter(DataType.of(new TypeReference<List<CharSequence>>() { })));
	}

	@Test
	void unboundedTypeVariableMatchesTypeVariable() {
		assertFalse(new TypeVariable("A")
			.isAssignableFrom(new TypeVariable("B")));
		assertTrue(new TypeVariable("A")
			.isAssignableFromGenericParameter(new TypeVariable("B")));
	}

	@Test
	void unboundedTypeVariableMatchesTypeVariableWithBound() {
		assertFalse(new TypeVariable("A")
			.isAssignableFrom(new TypeVariable("B", String.class)));
		assertTrue(new TypeVariable("A")
			.isAssignableFromGenericParameter(new TypeVariable("B", String.class)));
	}

	@Test
	void boundedTypeVariableDoesNotMatchUnboundedTypeVariable() {
		assertFalse(new TypeVariable("A", String.class)
			.isAssignableFrom(new TypeVariable("B")));
		assertFalse(new TypeVariable("A", String.class)
			.isAssignableFromGenericParameter(new TypeVariable("B")));
	}

	@Test
	void boundedTypeVariableMatchesTypeVariableWithSubtypeBound() {
		assertFalse(new TypeVariable("A", CharSequence.class)
			.isAssignableFrom(new TypeVariable("B", String.class)));
		assertTrue(new TypeVariable("A", CharSequence.class)
			.isAssignableFromGenericParameter(new TypeVariable("B", String.class)));
	}

	@Test
	void boundedTypeVariableDoesNotMatchTypeVariableWithSupertypeBound() {
		assertFalse(new TypeVariable("A", String.class)
			.isAssignableFrom(new TypeVariable("B", CharSequence.class)));
		assertFalse(new TypeVariable("A", String.class)
			.isAssignableFromGenericParameter(new TypeVariable("B", CharSequence.class)));
	}

	@Test
	<X extends CharSequence & Constable> void typeVariableWithMultipleBoundsMatchesSubtype() {
		assertFalse(DataType.of(new TypeReference<X>(){})
			.isAssignableFrom(DataType.of(String.class)));
		assertTrue(DataType.of(new TypeReference<X>(){})
			.isAssignableFromGenericParameter(DataType.of(String.class)));
	}

	@Test
	<X extends CharSequence & Constable> void typeVariableWithMultipleBoundsDoesNotMatchTypeSatisfyingJustOneBound() {
		assertFalse(DataType.of(new TypeReference<X>(){})
			.isAssignableFrom(DataType.of(Integer.class)));
		assertFalse(DataType.of(new TypeReference<X>(){})
			.isAssignableFromGenericParameter(DataType.of(Integer.class)));
	}

	@Test
	<S extends String> void stringMatchesTypeVariableBoundedToString() {
		assertTrue(DataType.STRING
			.isAssignableFrom(DataType.of(new TypeReference<S>() { })));
		assertFalse(DataType.STRING
				.isAssignableFromGenericParameter(DataType.of(new TypeReference<S>() { })),
			"Type argument assignability is not covariant");
	}

	@Test
	<C extends CharSequence> void stringDoesNotMatchTypeVariableBoundedOnlyToSupertype() {
		assertFalse(DataType.STRING
			.isAssignableFrom(DataType.of(new TypeReference<C>() { })));
		assertFalse(DataType.STRING
			.isAssignableFromGenericParameter(DataType.of(new TypeReference<C>() { })));
	}

	@Test
	<X extends CharSequence & Constable> void objectMatchesTypeVariableBoundedToMultipleSubtypes() {
		assertTrue(DataType.OBJECT
			.isAssignableFrom(DataType.of(new TypeReference<X>() { })));
		assertFalse(DataType.OBJECT
				.isAssignableFromGenericParameter(DataType.of(new TypeReference<X>() { })),
			"Type argument assignability is not covariant");
	}

	@Test
	<X extends CharSequence & Constable> void stringDoesNotMatchTypeVariableBoundedOnlyToMultipleSupertypes() {
		assertFalse(DataType.STRING
			.isAssignableFrom(DataType.of(new TypeReference<X>() { })));
		assertFalse(DataType.STRING
			.isAssignableFromGenericParameter(DataType.of(new TypeReference<X>() { })));
	}

	@Test
	<X extends CharSequence & Constable> void charSequenceMatchesTypeVariableIfJustOneBoundMatches() {
		assertTrue(DataType.of(CharSequence.class)
			.isAssignableFrom(DataType.of(new TypeReference<X>() { })));
		assertFalse(DataType.of(CharSequence.class)
				.isAssignableFromGenericParameter(DataType.of(new TypeReference<X>() { })),
			"Type argument assignability is not covariant");
	}

	@Test
	<V> void unboundedTypeVariableMatchesAnything() {
		assertFalse(DataType.of(new TypeReference<List<V>>() { })
			.isAssignableFrom(new TypeReference<List<String>>() { }));
		assertTrue(DataType.of(new TypeReference<List<V>>() { })
			.isAssignableFromGenericParameter(DataType.of(new TypeReference<List<String>>() { })));
	}

	@Test
	<C extends CharSequence> void boundedTypeVariableMatchesBound() {
		assertFalse(DataType.of(new TypeReference<List<C>>() { })
			.isAssignableFrom(new TypeReference<List<CharSequence>>() { }));
		assertTrue(DataType.of(new TypeReference<List<C>>() { })
			.isAssignableFromGenericParameter(DataType.of(new TypeReference<List<CharSequence>>() { })));
	}

	@Test
	<C extends CharSequence> void boundedTypeVariableMatchesSubtype() {
		assertFalse(DataType.of(new TypeReference<List<C>>() { })
			.isAssignableFrom(new TypeReference<List<String>>() { }));
		assertTrue(DataType.of(new TypeReference<List<C>>() { })
			.isAssignableFromGenericParameter(DataType.of(new TypeReference<List<String>>() { })));
	}

	@Test
	<C extends CharSequence> void boundedTypeVariableDoesNotMatchUnrelatedType() {
		assertFalse(DataType.of(new TypeReference<List<C>>() { })
			.isAssignableFrom(new TypeReference<List<Integer>>() { }));
		assertFalse(DataType.of(new TypeReference<List<C>>() { })
			.isAssignableFromGenericParameter(DataType.of(new TypeReference<List<Integer>>() { })));
	}

	@Test
	<S extends String> void boundedTypeVariableDoesNotMatchSupertype() {
		assertFalse(DataType.of(new TypeReference<List<S>>() { })
			.isAssignableFrom(new TypeReference<List<CharSequence>>() { }));
		assertFalse(DataType.of(new TypeReference<List<S>>() { })
			.isAssignableFromGenericParameter(DataType.of(new TypeReference<List<CharSequence>>() { })));
	}

	@Test
	void rawTypeMatchesParameterizedType() {
		assertTrue(DataType.of(List.class)
			.isAssignableFrom(DataType.of(new TypeReference<List<String>>() { })));
		assertTrue(DataType.of(List.class)
			.isAssignableFromGenericParameter(DataType.of(new TypeReference<List<String>>() { })));
		assertTrue(DataType.of(new TypeReference<List<String>>() { })
			.isAssignableFrom(DataType.of(List.class)));
		assertTrue(DataType.of(new TypeReference<List<String>>() { })
			.isAssignableFromGenericParameter(DataType.of(List.class)));
		assertTrue(DataType.of(Iterable.class)
			.isAssignableFrom(DataType.of(new TypeReference<List<String>>() { })));
	}

	@Test
	void upperBoundedWildcardComparedToUpperBoundedWildcard() {
		assertTrue(DataType.of(new TypeReference<List<? extends CharSequence>>() { })
			.isAssignableFrom(DataType.of(new TypeReference<List<? extends String>>() { })));
		assertTrue(DataType.of(new TypeReference<List<? extends CharSequence>>() { })
			.isAssignableFromGenericParameter(DataType.of(new TypeReference<List<? extends String>>() { })));
	}

	@Test
	void lowerBoundedWildcardComparedToLowerBoundedWildcard() {
		assertTrue(DataType.of(new TypeReference<List<? super String>>() { })
			.isAssignableFrom(DataType.of(new TypeReference<List<? super CharSequence>>() { })));
		assertTrue(DataType.of(new TypeReference<List<? super String>>() { })
			.isAssignableFromGenericParameter(DataType.of(new TypeReference<List<? super CharSequence>>() { })));
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

		assertFalse(DataType.of(Object[].class).isAssignableFromGenericParameter(DataType.of(String[].class)));
		assertFalse(DataType.of(String[].class).isAssignableFromGenericParameter(DataType.of(Object[].class)));
	}

	@Test
	<T> void arrayVsNonArray() {
		assertFalse(DataType.of(String[].class)
			.isAssignableFrom(DataType.of(Object.class)));
		assertFalse(DataType.of(String[].class)
			.isAssignableFromGenericParameter(DataType.of(Object.class)));

		assertFalse(DataType.of(new TypeReference<T[]>(){})
			.isAssignableFrom(DataType.of(Object.class)));
		assertFalse(DataType.of(new TypeReference<T[]>(){})
			.isAssignableFromGenericParameter(DataType.of(Object.class)));
	}

	@Test
	void arrayIsNotAssignableToUnrelatedInterface() {
		assertFalse(DataType.of(List.class).isAssignableFrom(DataType.of(String[].class)));
	}

	@Test
	void primitiveArrayNotAssignableToObjectArray() {
		assertFalse(DataType.of(Object[].class).isAssignableFrom(DataType.of(int[].class)));
		assertFalse(DataType.of(Object[].class).isAssignableFromGenericParameter(DataType.of(int[].class)));
	}

	@Test
	<C extends CharSequence> void knownVsUnknownArray() {
		var arrayOfObject = DataType.of(new TypeReference<Object[]>() { });
		var arrayOfCharSequence = DataType.of(new TypeReference<CharSequence[]>() { });
		var arrayOfString = DataType.of(new TypeReference<String[]>() { });
		var arrayOfC = DataType.of(new TypeReference<C[]>() { });

		assertTrue(arrayOfObject.isAssignableFrom(arrayOfC));
		assertFalse(arrayOfObject.isAssignableFromGenericParameter(arrayOfC));

		assertTrue(arrayOfCharSequence.isAssignableFrom(arrayOfC));
		assertFalse(arrayOfCharSequence.isAssignableFromGenericParameter(arrayOfC));

		assertFalse(arrayOfString.isAssignableFrom(arrayOfC));
		assertFalse(arrayOfString.isAssignableFromGenericParameter(arrayOfC));

		assertFalse(arrayOfC.isAssignableFrom(arrayOfObject));
		assertFalse(arrayOfC.isAssignableFromGenericParameter(arrayOfObject));

		assertFalse(arrayOfC.isAssignableFrom(arrayOfCharSequence));
		assertTrue(arrayOfC.isAssignableFromGenericParameter(arrayOfCharSequence));

		assertFalse(arrayOfC.isAssignableFrom(arrayOfString));
		assertTrue(arrayOfC.isAssignableFromGenericParameter(arrayOfString));
	}

	@Test
	void parameterTypeMappingThroughInheritance() {
		assertTrue(DataType.of(new TypeReference<List<String>>() { })
			.isAssignableFrom(DataType.of(new TypeReference<ArrayList<String>>() { })));
		assertFalse(DataType.of(new TypeReference<List<String>>() { })
			.isAssignableFromGenericParameter(DataType.of(new TypeReference<ArrayList<String>>() { })));
	}

	@Test
	<T> void arrayOfUnboundedVariable() {
	    var arrayOfT = DataType.of(new TypeReference<T[]>() { });
		DataType arrayOfString = DataType.of(String[].class);
		assertFalse(arrayOfT.isAssignableFrom(arrayOfString));
	    assertTrue(arrayOfT.isAssignableFromGenericParameter(arrayOfString));
	}

	@Test
	<T extends CharSequence> void arrayOfBoundedVariable() {
	    var arrayOfT = DataType.of(new TypeReference<T[]>() { });

		DataType arrayOfString = DataType.of(String[].class);
	    assertFalse(arrayOfT.isAssignableFrom(arrayOfString));
	    assertTrue(arrayOfT.isAssignableFromGenericParameter(arrayOfString));

		DataType arrayOfObject = DataType.of(Object[].class);
	    assertFalse(arrayOfT.isAssignableFrom(arrayOfObject));
	    assertFalse(arrayOfT.isAssignableFromGenericParameter(arrayOfObject));
	}

	@Test
	<X extends CharSequence & Constable> void arrayOfMultiBoundedVariable() {
		DataType arrayOfX = DataType.of(new TypeReference<X[]>() { });
		assertFalse(arrayOfX
			.isAssignableFrom(DataType.of(String[].class)));
		assertTrue(arrayOfX
			.isAssignableFromGenericParameter(DataType.of(String[].class)));

		// Integer is Constable but not CharSequence
		assertFalse(arrayOfX
			.isAssignableFrom(DataType.of(Integer[].class)));
		assertFalse(arrayOfX
			.isAssignableFromGenericParameter(DataType.of(Integer[].class)));
	}

	@Test
	void arrayOfUnboundedWildcard() {
		var arrayOfWildcard = new UnknownArrayType(new UnboundedWildcardType());
		var arrayofString = DataType.of(new TypeReference<String[]>() { });
		assertTrue(arrayOfWildcard
			.isAssignableFrom(arrayofString));
		assertTrue(arrayOfWildcard
			.isAssignableFromGenericParameter(arrayofString));
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
			.isAssignableFromGenericParameter(arrayOfObject));

		var arrayOfCharSequence = DataType.of(new TypeReference<CharSequence[]>() { });
		assertTrue(arrayOfWildcard
			.isAssignableFrom(arrayOfCharSequence));
		assertTrue(arrayOfWildcard
			.isAssignableFromGenericParameter(arrayOfCharSequence));

		var arrayOfString = DataType.of(new TypeReference<String[]>() { });
		assertTrue(arrayOfWildcard
			.isAssignableFrom(arrayOfString));
		assertTrue(arrayOfWildcard
			.isAssignableFromGenericParameter(arrayOfString));
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
			.isAssignableFromGenericParameter(arrayOfObject));

		var arrayOfCharSequence = DataType.of(new TypeReference<CharSequence[]>() { });
		assertTrue(arrayOfWildcard
			.isAssignableFrom(arrayOfCharSequence));
		assertTrue(arrayOfWildcard
			.isAssignableFromGenericParameter(arrayOfCharSequence));

		var arrayofString = DataType.of(new TypeReference<String[]>() { });
		assertFalse(arrayOfWildcard
			.isAssignableFrom(arrayofString));
		assertFalse(arrayOfWildcard
			.isAssignableFromGenericParameter(arrayofString));
	}

	@Test
	void wrongErasedTypeDoesNotMatch() {
		assertFalse(DataType.of(new TypeReference<List<String>>(){})
			.isAssignableFrom(DataType.of(Map.class)));
		assertFalse(DataType.of(new TypeReference<List<String>>(){})
			.isAssignableFromGenericParameter(DataType.of(Map.class)));
	}

	@Test
	void rightErasedTypeMatches() {
		assertTrue(DataType.of(new TypeReference<List<String>>(){})
			.isAssignableFrom(DataType.of(ArrayList.class)));
		assertFalse(DataType.of(new TypeReference<List<String>>(){})
			.isAssignableFromGenericParameter(DataType.of(ArrayList.class)));
	}

	@Test
	<T> void invariantListWithTypeVariable() {
	    var listT = DataType.of(new TypeReference<List<T>>() { });
	    var listString = DataType.of(new TypeReference<List<String>>() { });

	    // Assignment to a variable of type List<T> should NOT accept List<String>
	    assertFalse(listT.isAssignableFrom(listString),
	      "List<T>.isAssignableFrom(List<String>) must be false (invariant for parameterized types)");

	    // But type-argument conformance (passing List<String> as a type-argument to List<T>) remains true
	    assertTrue(listT.isAssignableFromGenericParameter(listString),
	      "List<T>.isAssignableFromGenericParameter(List<String>) should be true (type-argument/bounds semantics)");
	}
}
