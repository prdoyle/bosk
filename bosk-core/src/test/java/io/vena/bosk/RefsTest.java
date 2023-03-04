package io.vena.bosk;

import io.vena.bosk.bytecode.ClassBuilder;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import static io.vena.bosk.ReferenceUtils.rawClass;
import static io.vena.bosk.bytecode.ClassBuilder.here;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RefsTest extends AbstractBoskTest {

	@Test
	void test() throws InvalidTypeException {
		Identifier parentID = Identifier.from("parent");
		Bosk<TestRoot> bosk = setUpBosk(Bosk::simpleDriver);
		TestEntityBuilder teb = new TestEntityBuilder(bosk);
		Refs refs = generateRefs(Refs.class, bosk);
		assertEquals(bosk.rootReference(), refs.root());
		assertEquals(teb.entityRef(parentID), refs.anyEntity().boundTo(parentID));
		assertEquals(teb.entityRef(parentID), refs.entity(parentID));
	}

	private static <T> T generateRefs(Class<T> refsClass, Bosk<TestRoot> bosk) throws InvalidTypeException {
		ClassBuilder<T> cb = new ClassBuilder<>(
			"REFS_" + refsClass.getSimpleName(),
			refsClass,
			refsClass.getClassLoader(),
			here()
		);

		cb.beginClass();

		for (Method method: refsClass.getDeclaredMethods()) {
			ReferencePath path = method.getAnnotation(ReferencePath.class);
			if (path == null) {
				continue;
			}
			Type returnType = method.getGenericReturnType();
			Type targetType = ReferenceUtils.parameterType(returnType, Reference.class, 0);
			cb.beginMethod(method);
			Reference<?> result;
			try {
				result = bosk.reference(rawClass(targetType), Path.parseParameterized(path.value()));
			} catch (InvalidTypeException e) {
				// Add some troubleshooting info for the user
				throw new InvalidTypeException("Reference type mismatch on " + methodName(method) + ": " + e.getMessage(), e);
			}
			cb.pushObject(result);
			switch (method.getParameterCount()) {
				case 0:
					// All is well
					break;
				case 1:
					if (!Identifier[].class.isAssignableFrom(method.getParameterTypes()[0])) {
						throw new InvalidTypeException("Parameter type must be Identifier[] or Identifier... on " + methodName(method));
					}
					cb.pushLocal(cb.parameter(1));
					cb.invoke(REFERENCE_BOUND_TO);
					break;
				default:
					throw new InvalidTypeException("Invalid number of parameters on " + methodName(method));
			}
			cb.finishMethod();
		}
		return cb.buildInstance();
	}

	@NotNull
	private static String methodName(Method method) {
		return method.getDeclaringClass().getSimpleName() + "." + method.getName();
	}

	public interface Refs {
		@ReferencePath("/")
		Reference<TestRoot> root();

		@ReferencePath("/entities/-entity-")
		Reference<TestEntity> anyEntity();

		@ReferencePath("/entities/-entity-")
		Reference<TestEntity> entity(Identifier... ids);
	}

	private static final Method REFERENCE_BOUND_TO;

	static {
		try {
			REFERENCE_BOUND_TO = Reference.class.getDeclaredMethod("boundTo", Identifier[].class);
		} catch (NoSuchMethodException e) {
			throw new AssertionError(e);
		}
	}
}
