package io.vena.bosk;

import io.vena.bosk.bytecode.ClassBuilder;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

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
			cb.beginMethod(method);
			Reference<?> result = bosk.reference(Object.class, Path.parseParameterized(path.value()));
			cb.pushObject(result);
			cb.finishMethod();
		}
		return cb.buildInstance();
	}

	public interface Refs {
		@ReferencePath("/")
		Reference<TestRoot> root();

		@ReferencePath("/entities/-entity-")
		Reference<TestEntity> anyEntity();
	}

}
