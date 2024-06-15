package io.vena.bosk.util;

import java.lang.reflect.Method;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReflectionHelpersTest {

	@Test
	void getDeclaredMethodsInOrder_correctOrder() throws NoSuchMethodException {
		List<Method> actual = ReflectionHelpers.getDeclaredMethodsInOrder(ClassWithSomeMethods.class);
		List<Method> expected = List.of(
			ClassWithSomeMethods.class.getDeclaredMethod("first"),
			ClassWithSomeMethods.class.getDeclaredMethod("secondOverloaded", int.class),
			ClassWithSomeMethods.class.getDeclaredMethod("secondOverloaded", long.class),
			ClassWithSomeMethods.class.getDeclaredMethod("secondOverloaded", float.class),
			ClassWithSomeMethods.class.getDeclaredMethod("secondOverloaded", String.class),
			ClassWithSomeMethods.class.getDeclaredMethod("third")
		);
		assertEquals(expected, actual);
	}

	@Test
	void getDeclaredMethodsInOrder_worksWithBuiltInClasses() throws NoSuchMethodException {
		List<Method> actual = ReflectionHelpers.getDeclaredMethodsInOrder(Runnable.class);
		List<Method> expected = List.of(
			Runnable.class.getDeclaredMethod("run")
		);
		assertEquals(expected, actual);
	}

	public interface ClassWithSomeMethods {
		void first();
		void secondOverloaded(int x);
		void secondOverloaded(long x);
		void secondOverloaded(float x);
		void secondOverloaded(String s);
		void third();
	}

}