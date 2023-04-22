package io.vena.bosk;

import io.vena.bosk.ReferenceUtils.CatalogRef;
import io.vena.bosk.ReferenceUtils.ListingRef;
import io.vena.bosk.ReferenceUtils.SideTableRef;
import io.vena.bosk.annotations.ReferencePath;
import io.vena.bosk.bytecode.ClassBuilder;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import org.jetbrains.annotations.NotNull;

import static io.vena.bosk.ReferenceUtils.parameterType;
import static io.vena.bosk.ReferenceUtils.rawClass;
import static io.vena.bosk.bytecode.ClassBuilder.here;
import static io.vena.bosk.util.Classes.catalog;
import static io.vena.bosk.util.Classes.listing;
import static io.vena.bosk.util.Classes.sideTable;

class ReferenceBuilder {
	@SuppressWarnings({"unchecked","rawtypes"})
	static <T, R extends Entity> T buildReferences(Class<T> refsClass, Bosk<R> bosk) throws InvalidTypeException {
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
			if (!Reference.class.isAssignableFrom(returnClass)) {
				throw new InvalidTypeException("Expected " + methodName(method) + " to return a Reference");
			}
			Type targetType = parameterType(returnType, Reference.class, 0);
			cb.beginMethod(method);
			Reference<?> result;
			try {
				Path path = Path.parseParameterized(referencePath.value());
				if (returnClass.equals(CatalogReference.class)) {
					Class entryClass = rawClass(parameterType(returnType, CatalogReference.class, 0));
					result = CatalogRef.of(bosk.reference(catalog(entryClass), path), entryClass);
				} else if (returnClass.equals(ListingReference.class)) {
					Class entryClass = rawClass(parameterType(returnType, ListingReference.class, 0));
					result = ListingRef.of(bosk.reference(listing(entryClass), path));
				} else if (returnClass.equals(SideTableReference.class)) {
					Class keyClass = rawClass(parameterType(returnType, SideTableReference.class, 0));
					Class valueClass = rawClass(parameterType(returnType, SideTableReference.class, 1));
					result = SideTableRef.of(bosk.reference(sideTable(keyClass, valueClass), path), keyClass, valueClass);
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

	static final Method REFERENCE_BOUND_TO_ARRAY;
	static final Method REFERENCE_BOUND_TO_ID;

	static {
		try {
			REFERENCE_BOUND_TO_ARRAY = Reference.class.getDeclaredMethod("boundTo", Identifier[].class);
			REFERENCE_BOUND_TO_ID = Runtime.class.getDeclaredMethod("boundTo", Reference.class, Identifier.class);
		} catch (NoSuchMethodException e) {
			throw new AssertionError(e);
		}
	}

	public static final class Runtime {
		public static Reference<?> boundTo(Reference<?> ref, Identifier id) {
			return ref.boundTo(id);
		}
	}

}
