package works.bosk;

import java.lang.reflect.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReferenceUtilsTest_parameterType {

	@Test
	void testDirectInterfaceParameters() throws SecurityException {
		Type type = new BinaryInterface<String,Integer>() {}.getClass().getGenericInterfaces()[0];
		checkStringAndIntegerParameters(type, BinaryInterface.class);
	}

	@Test
	void testDirectClassParameters() throws SecurityException {
		@SuppressWarnings("unused")
		abstract class BinaryClass<T0,T1> {}
		Type type = new BinaryClass<String,Integer>() {}.getClass().getGenericSuperclass();
		checkStringAndIntegerParameters(type, BinaryClass.class);
	}

	@Test
	void testExtendedClassConcrete() throws SecurityException {
		@SuppressWarnings("unused")
		abstract class BinaryClass<T0,T1> {}

		// BinaryClass parameters are concrete types
		abstract class Subclass extends BinaryClass<String, Integer> {}

		Type type = new Subclass() {}.getClass().getGenericSuperclass();
		checkStringAndIntegerParameters(type, BinaryClass.class);
	}

	@Test
	void testImplementedInterfaceConcrete() throws SecurityException {
		// Binary parameters are concrete types
		abstract class Subclass implements BinaryInterface<String, Integer> {}

		Type type = new Subclass() {}.getClass().getGenericSuperclass();
		checkStringAndIntegerParameters(type, BinaryInterface.class);
	}

	@Test
	void testExtendedInterfaceConcrete() throws SecurityException {
		Type type = new ConcreteSub() {}.getClass().getGenericInterfaces()[0];
		checkStringAndIntegerParameters(type, BinaryInterface.class);
	}

	@Test
	void testExtendedClassVariable() throws SecurityException {
		@SuppressWarnings("unused")
		abstract class BinaryClass<T0,T1> {}

		// BinaryClass parameters are type variables
		abstract class Subclass<T0,T1> extends BinaryClass<T0, T1> {}

		Type type = new Subclass<String, Integer>() {}.getClass().getGenericSuperclass();
		checkStringAndIntegerParameters(type, BinaryClass.class);
	}

	@Test
	void testImplementedInterfaceVariable() throws SecurityException {
		// Binary parameters are type variables
		abstract class Subclass<T0,T1> implements BinaryInterface<T0, T1> {}

		Type type = new Subclass<String, Integer>() {}.getClass().getGenericSuperclass();
		checkStringAndIntegerParameters(type, BinaryInterface.class);
	}

	@Test
	void testExtendedInterfaceVariable() throws SecurityException {
		Type type = new VariableSub<String, Integer>() {}.getClass().getGenericInterfaces()[0];
		checkStringAndIntegerParameters(type, BinaryInterface.class);
	}

	@Test
	void testExtendedClassSwitcheroo() throws SecurityException {
		@SuppressWarnings("unused")
		abstract class BinaryClass<T0,T1> {}

		// Type parameters are reversed
		abstract class Subclass<T0,T1> extends BinaryClass<T1, T0> {}

		Type type = new Subclass<Integer, String>() {}.getClass().getGenericSuperclass();
		checkStringAndIntegerParameters(type, BinaryClass.class);
	}

	@Test
	void testParameterWithParameters() throws NoSuchFieldException {
		abstract class TestClass {
			public SideTableReference<Entity, String> sideTable;
		}
		Type type = TestClass.class.getDeclaredField("sideTable").getGenericType();
		Type targetType = ReferenceUtils.parameterType(type, Reference.class, 0);
		Type keyType = ReferenceUtils.parameterType(targetType, SideTable.class, 0);
		Type valueType = ReferenceUtils.parameterType(targetType, SideTable.class, 1);
		assertEquals(Entity.class, keyType);
		assertEquals(String.class, valueType);
	}

	private void checkStringAndIntegerParameters(Type type, Class<?> expectedClass) {
		Assertions.assertEquals(String.class,  ReferenceUtils.parameterType(type, expectedClass, 0));
		Assertions.assertEquals(Integer.class, ReferenceUtils.parameterType(type, expectedClass, 1));
		assertThrows(IndexOutOfBoundsException.class, ()-> ReferenceUtils.parameterType(type, expectedClass, -1));
		assertThrows(IndexOutOfBoundsException.class, ()-> ReferenceUtils.parameterType(type, expectedClass, 2));
	}

	// Can't declare method-local interfaces

	@SuppressWarnings("unused") // Type parameters T0 and T1 aren't used, but that's ok
	interface BinaryInterface<T0,T1> {}

	interface ConcreteSub extends BinaryInterface<String, Integer> {}

	interface VariableSub<T0,T1> extends BinaryInterface<T0, T1> {}
}
