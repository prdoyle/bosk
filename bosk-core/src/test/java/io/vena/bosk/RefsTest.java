package io.vena.bosk;

import io.vena.bosk.bytecode.ClassBuilder;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import static io.vena.bosk.ReferenceUtils.parameterType;
import static io.vena.bosk.ReferenceUtils.rawClass;
import static io.vena.bosk.bytecode.ClassBuilder.here;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RefsTest extends AbstractBoskTest {

	@Test
	void test() throws InvalidTypeException {
		Identifier parentID = Identifier.from("parent");
		Identifier childID = Identifier.from("child");
		Bosk<TestRoot> bosk = setUpBosk(Bosk::simpleDriver);
		TestEntityBuilder teb = new TestEntityBuilder(bosk);
		Refs refs = generateRefs(Refs.class, bosk);
		assertEquals(bosk.rootReference(), refs.root());
		assertEquals(teb.entityRef(parentID), refs.anyEntity().boundTo(parentID));
		assertEquals(teb.entityRef(parentID), refs.entity(parentID));
		assertEquals(teb.childrenRef(parentID).then(childID), refs.child(parentID, childID));
		assertEquals(teb.childrenRef(parentID), refs.children(parentID));
		assertEquals(teb.entityRef(parentID).thenListing(TestChild.class, "oddChildren"), refs.oddChildren(parentID));
		assertEquals(teb.entityRef(parentID).thenSideTable(TestChild.class, String.class, "stringSideTable"), refs.stringSideTable(parentID));
	}

	public interface Refs {
		@ReferencePath("/")
		Reference<TestRoot> root();

		@ReferencePath("/entities/-entity-")
		Reference<TestEntity> anyEntity();

		@ReferencePath("/entities/-entity-")
		Reference<TestEntity> entity(Identifier... ids);

		@ReferencePath("/entities/-entity-/children/-child-")
		Reference<TestChild> child(Identifier entity, Identifier child);

		@ReferencePath("/entities/-entity-/children")
		CatalogReference<TestChild> children(Identifier entity);

		@ReferencePath("/entities/-entity-/oddChildren")
		ListingReference<TestChild> oddChildren(Identifier entity);

		@ReferencePath("/entities/-entity-/stringSideTable")
		SideTableReference<TestChild,String> stringSideTable(Identifier parentID);
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private static <T> T generateRefs(Class<T> refsClass, Bosk<TestRoot> bosk) throws InvalidTypeException {
		ClassBuilder<T> cb = new ClassBuilder<>(
			"REFS_" + refsClass.getSimpleName(),
			refsClass,
			refsClass.getClassLoader(),
			here()
		);

		cb.beginClass();

		for (Method method: refsClass.getDeclaredMethods()) {
			ReferencePath referencePath = method.getAnnotation(ReferencePath.class);
			if (referencePath == null) {
				throw new InvalidTypeException("Missing " + ReferencePath.class.getSimpleName() + " annotation on " + methodName(method));
			}
			Type returnType = method.getGenericReturnType();
			Class<?> returnClass = rawClass(returnType);
			Type targetType = parameterType(returnType, Reference.class, 0);
			cb.beginMethod(method);
			Reference<?> result;
			try {
				Path path = Path.parseParameterized(referencePath.value());
				if (returnClass.equals(CatalogReference.class)) {
					Type entryType = parameterType(returnType, CatalogReference.class, 0);
					result = bosk.catalogReference((Class) rawClass(entryType), path);
				} else if (returnClass.equals(ListingReference.class)) {
					Type entryType = parameterType(returnType, ListingReference.class, 0);
					result = bosk.listingReference((Class) rawClass(entryType), path);
				} else if (returnClass.equals(SideTableReference.class)) {
					Type keyType = parameterType(returnType, SideTableReference.class, 0);
					Type valueType = parameterType(returnType, SideTableReference.class, 1);
					result = bosk.sideTableReference((Class) rawClass(keyType), (Class) rawClass(valueType), path);
				} else {
					result = bosk.reference(rawClass(targetType), path);
				}
			} catch (InvalidTypeException e) {
				// Add some troubleshooting info for the user
				throw new InvalidTypeException("Reference type mismatch on " + methodName(method) + ": " + e.getMessage(), e);
			}
			cb.pushObject(result);
			int parameterIndex = 0;
			for (Parameter p: method.getParameters()) {
				++parameterIndex;
				if (Identifier.class.isAssignableFrom(p.getType())) {
					cb.pushLocal(cb.parameter(parameterIndex));
					cb.invoke(REFERENCE_BOUND_TO_ID);
				} else if (Identifier[].class.isAssignableFrom(p.getType())) {
					cb.pushLocal(cb.parameter(parameterIndex));
					cb.invoke(REFERENCE_BOUND_TO_ARRAY);
				} else {
					throw new InvalidTypeException("Unexpected parameter type " + p.getType().getSimpleName() + " on " + methodName(method));
				}
			}
			cb.finishMethod();
		}
		return cb.buildInstance();
	}

	@NotNull
	private static String methodName(Method method) {
		return method.getDeclaringClass().getSimpleName() + "." + method.getName();
	}

	public static final class Runtime {
		public static Reference<?> boundTo(Reference<?> ref, Identifier id) {
			return ref.boundTo(id);
		}
	}

	private static final Method REFERENCE_BOUND_TO_ARRAY;
	private static final Method REFERENCE_BOUND_TO_ID;

	static {
		try {
			REFERENCE_BOUND_TO_ARRAY = Reference.class.getDeclaredMethod("boundTo", Identifier[].class);
			REFERENCE_BOUND_TO_ID = Runtime.class.getDeclaredMethod("boundTo", Reference.class, Identifier.class);
		} catch (NoSuchMethodException e) {
			throw new AssertionError(e);
		}
	}
}
