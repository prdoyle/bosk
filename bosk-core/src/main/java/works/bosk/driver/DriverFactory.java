package works.bosk.driver;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Stream;
import works.bosk.BoskDriver;

import static java.util.Collections.emptyList;
import static works.bosk.ReferenceUtils.rawClass;

/**
 * Describes a way to create a {@link works.bosk.BoskDriver} object.
 * Includes a {@link MethodHandle} plus some additional metadata.
 *
 * @param methodHandle the handle to invoke in order to create the {@code BoskDriver}
 * @param parameters a {@link ParameterSpec} for each parameter of the {@code methodHandle}
 */
public record DriverFactory(
	MethodHandle methodHandle,
	List<ParameterSpec> parameters
) {
	@SuppressWarnings("unchecked")
	public Class<? extends BoskDriver> returnClass() {
		return (Class<? extends BoskDriver>) methodHandle.type().returnType();
	}

	/**
	 * @param c must be public
	 */
	public static DriverFactory ofConstructor(Constructor<?> c) {
		try {
			return new DriverFactory(
				MethodHandles.publicLookup().unreflectConstructor(c),
				Stream.of(c.getParameters()).map(ParameterSpec::of).toList()
			);
		} catch (IllegalAccessException e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * @param m must be public
	 */
	public static DriverFactory ofVirtualMethod(Method m, Object receiver) {
		try {
			return new DriverFactory(
				MethodHandles.publicLookup().unreflect(m).bindTo(receiver),
				Stream.of(m.getParameters()).map(ParameterSpec::of).toList());
		} catch (IllegalAccessException e) {
			throw new IllegalArgumentException(e);
		}
	}

	public static DriverFactory ofInstance(BoskDriver driver) {
		return new DriverFactory(
			MethodHandles.constant(driver.getClass(), driver),
			emptyList());
	}

	public DriverFactory {
		var methodType = methodHandle.type();
		Class<?> returnClass = methodType.returnType();
		if (!BoskDriver.class.isAssignableFrom(returnClass)) {
			throw new IllegalArgumentException("ReturnType " + returnClass.getSimpleName() + " is not a BoskDriver");
		}
		if (parameters.size() != methodType.parameterCount()) {
			throw new IllegalArgumentException("MethodHandle has "
				+ methodType.parameterCount()
				+ " parameters; expected "
				+ parameters.size());
		}
		for (int i = 0; i < parameters.size(); i++) {
			var spec = parameters.get(i);
			var type = methodType.parameterType(i);
			Class<?> specClass = rawClass(spec.type());
			if (!type.isAssignableFrom(specClass)) {
				throw new IllegalArgumentException(
					"MethodHandle parameter \""
					+ spec.name()
					+ "\" type "
					+ type.getSimpleName()
					+ " is incompatible with "
					+ specClass.getSimpleName()
				);
			}
		}
	}
}
