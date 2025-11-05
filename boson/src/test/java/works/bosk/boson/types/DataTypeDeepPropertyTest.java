package works.bosk.boson.types;

import java.lang.reflect.Parameter;
import java.util.List;
import works.bosk.boson.codec.PrimitiveTypeInjector;
import works.bosk.junit.InjectFrom;
import works.bosk.junit.InjectedTest;
import works.bosk.junit.ParameterInjector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests properties of {@link DataType} including any child types it may have.
 */
@InjectFrom({
	PrimitiveTypeInjector.class,
	DataTypeDeepPropertyTest.InstanceTypeInjector.class,
	DataTypeDeepPropertyTest.UnknownTypeInjector.class
})
public class DataTypeDeepPropertyTest {

	@InjectedTest
	void primitiveType(PrimitiveType dataType) {
		assertTrue(dataType.isFullyKnown());
		assertFalse(dataType.hasWildcards());
	}

	@InjectedTest
	void instanceType(InstanceTypeInjector.Case _case) {
		assertEquals(_case.isFullyKnown(), _case.type.isFullyKnown());
		assertEquals(_case.hasWildcards(), _case.type.hasWildcards());
	}

	@InjectedTest
	void arrayType(InstanceTypeInjector.Case _case) {
		assertEquals(_case.isFullyKnown(), new ArrayType(_case.type).isFullyKnown());
		assertEquals(_case.hasWildcards(), new ArrayType(_case.type).hasWildcards());
	}

	@InjectedTest
	void primitiveArrayType(PrimitiveType primitiveType) {
		assertTrue(new ArrayType(primitiveType).isFullyKnown());
		assertFalse(new ArrayType(primitiveType).hasWildcards());
	}

	@InjectedTest
	void unknownType(UnknownTypeInjector.Case _case) {
		assertEquals(_case.isFullyKnown(), _case.type.isFullyKnown());
		assertEquals(_case.hasWildcards(), _case.type.hasWildcards());
	}

	@InjectedTest
	void unknownArrayType(UnknownTypeInjector.Case _case) {
		assertEquals(_case.isFullyKnown(), new UnknownArrayType(_case.type).isFullyKnown());
		assertEquals(_case.hasWildcards(), new UnknownArrayType(_case.type).hasWildcards());
	}

	static class InstanceTypeInjector implements ParameterInjector {
		record Case(
			InstanceType type,
			boolean isFullyKnown,
			boolean hasWildcards
		) {}

		@Override
		public boolean supportsParameter(Parameter parameter) {
			return parameter.getType() == Case.class;
		}

		@Override
		public List<Object> values() {
			return values2();
		}

		@SuppressWarnings("rawtypes")
		private static <T> List<Object> values2() {
			return List.of(
				new Case(
					DataType.STRING,
					true,
					false
				),
				new Case(
					DataType.OBJECT,
					true,
					false
				),
				new Case(
					(InstanceType) DataType.of(new TypeReference<List<?>>() {
					}),
					false,
					true
				),
				new Case(
					(InstanceType) DataType.of(new TypeReference<List<String>>() {
					}),
					true,
					false
				),
				new Case(
					(InstanceType) DataType.of(new TypeReference<List<T>>() {
					}),
					false,
					false
				),
				new Case(
					(InstanceType) DataType.of(new TypeReference<List>() {
					}),
					false,
					true // Erased type is considered to have wildcards
				)
			);
		}
	}

	static class UnknownTypeInjector implements ParameterInjector {
		record Case(
			UnknownType type,
			boolean isFullyKnown,
			boolean hasWildcards
		) {}

		@Override
		public boolean supportsParameter(Parameter parameter) {
			return parameter.getType() == Case.class;
		}

		@Override
		public List<Object> values() {
			return values2();
		}

		private List<Object> values2() {
			return List.of(
				new Case(
					TypeVariable.unbounded("T"),
					false,
					false
				),
				new Case(
					new UnboundedWildcardType(),
					false,
					true
				),
				new Case(
					new UpperBoundedWildcardType(DataType.STRING),
					false,
					true
				),
				new Case(
					new LowerBoundedWildcardType(DataType.STRING),
					false,
					true
				)
			);
		}
	}
}
