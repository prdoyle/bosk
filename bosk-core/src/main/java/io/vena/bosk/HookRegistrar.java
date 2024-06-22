package io.vena.bosk;

import io.vena.bosk.annotations.Hook;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.vena.bosk.util.ReflectionHelpers.getDeclaredMethodsInOrder;
import static java.lang.reflect.Modifier.isPrivate;
import static java.lang.reflect.Modifier.isStatic;
import static java.util.Comparator.comparing;

@RequiredArgsConstructor
class HookRegistrar {
	static <T> void registerHooks(T receiverObject, Bosk<?> bosk) throws InvalidTypeException {
		List<Method> methods = hookMethods(receiverObject.getClass());
		if (methods.isEmpty()) {
			LOGGER.warn("Found no hook methods in {}; may be misconfigured", receiverObject.getClass().getSimpleName());
			return;
		}

		for (Method method : methods) {
			method.setAccessible(true);
			Path path = Path.parseParameterized(method.getAnnotation(Hook.class).value());

			Reference<?> plainRef = bosk.rootReference().then(Object.class, path);
			// Now substitute one of the handy Reference subtypes where possible
			Reference<?> scope;
			if (Catalog.class.isAssignableFrom(plainRef.targetClass())) {
				scope = bosk.rootReference().thenCatalog(Entity.class, path);
			} else if (Listing.class.isAssignableFrom(plainRef.targetClass())) {
				scope = bosk.rootReference().thenListing(Entity.class, path);
			} else if (SideTable.class.isAssignableFrom(plainRef.targetClass())) {
				scope = bosk.rootReference().thenSideTable(Entity.class, Object.class, path);
			} else {
				scope = plainRef;
			}

			List<Function<Reference<?>, Object>> argumentFunctions = new ArrayList<>(method.getParameterCount());
			argumentFunctions.add(ref -> receiverObject); // The "this" pointer
			for (Parameter p : method.getParameters()) {
				if (Reference.class.isAssignableFrom(p.getType())) {
					if (ReferenceUtils.parameterType(p.getParameterizedType(), Reference.class, 0).equals(scope.targetType())) {
						argumentFunctions.add(ref -> ref);
					} else {
						throw new IllegalArgumentException("Expected reference to " + scope.targetType() + ": " + method.getName() + " parameter " + p);
					}
				} else if (p.getType().isAssignableFrom(BindingEnvironment.class)) {
					argumentFunctions.add(ref -> scope.parametersFrom(ref.path()));
				} else {
					throw new IllegalArgumentException("Unsupported parameter type " + p.getType() + ": " + method.getName() + " parameter " + p);
				}
			}
			MethodHandle hook;
			try {
				hook = MethodHandles.lookup().unreflect(method);
			} catch (IllegalAccessException e) {
				throw new IllegalArgumentException(e);
			}
			bosk.registerHook(method.getName(), scope, ref -> {
				try {
					List<Object> arguments = new ArrayList<>(argumentFunctions.size());
					argumentFunctions.forEach(f -> arguments.add(f.apply(ref)));
					hook.invokeWithArguments(arguments);
				} catch (Throwable e) {
					throw new IllegalStateException("Unable to call hook \"" + method.getName() + "\"", e);
				}
			});
			LOGGER.debug("Registered hook {}", method);
		}
		int numHooks = methods.size();
		LOGGER.info("Registered {} hook{} in {}", numHooks, (numHooks >= 2) ? "s" : "", receiverObject.getClass().getSimpleName());
	}

	private static List<Method> hookMethods(Class<?> c) {
		Deque<Class<?>> descendingHierarchy = new ArrayDeque<>();
		for (
			Class<?> receiverClass = c;
			receiverClass != Object.class;
			receiverClass = receiverClass.getSuperclass()
		) {
			descendingHierarchy.addFirst(receiverClass);
		}
		List<Method> hookMethods = new ArrayList<>();
		for (Class<?> receiverClass: descendingHierarchy) {
			for (Method method : getDeclaredMethodsInOrder(receiverClass)) {
				Hook hookAnnotation = method.getAnnotation(Hook.class);
				if (hookAnnotation == null) {
					continue;
				} else {
					LOGGER.debug("Found hook {}: {}", method, hookAnnotation);
				}
				if (isStatic(method.getModifiers())) {
					throw new IllegalArgumentException("Hook method cannot be static: " + method);
				} else if (isPrivate(method.getModifiers())) {
					throw new IllegalArgumentException("Hook method cannot be private: " + method);
				}
				hookMethods.add(method);
			}
		}
		return hookMethods.stream()
			.sorted(comparing(HookRegistrar::hookPriority))
			.toList();
	}

	private static int hookPriority(Method method) {
		return -method.getAnnotation(Hook.class).priority();
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(HookRegistrar.class);
}
