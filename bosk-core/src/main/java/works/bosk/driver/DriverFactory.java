package works.bosk.driver;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Stream;
import works.bosk.BoskDriver;
import works.bosk.BoskInfo;

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
	 * @param m must be public
	 */
	static DriverFactory ofVirtualMethod(Method m, Object receiver) {
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

	public interface DriverAcceptingFunction {
		BoskDriver accept(BoskDriver downstream);
	}

	public static DriverFactory of(DriverAcceptingFunction f) {
		return new DriverFactory(ACCEPT_DRIVER.bindTo(f), ACCEPT_DRIVER_PARAMETERS);
	}

	public interface BoskInfoAcceptingFunction {
		BoskDriver accept(BoskInfo<?> boskInfo);
	}

	public static DriverFactory of(BoskInfoAcceptingFunction f) {
		return new DriverFactory(ACCEPT_BOSK_INFO.bindTo(f), ACCEPT_BOSK_INFO_PARAMETERS);
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

	public BoskDriver build(BoskInfo<?> boskInfo, BoskDriver downstream) {
		List<Object> args = parameters.stream()
			.map(t -> pickArgument(t, boskInfo, downstream))
			.toList();
		try {
			return (BoskDriver) methodHandle.invokeWithArguments(args);
		} catch (Error e) {
			// Don't mess with errors
			throw e;
		} catch (Throwable e) {
			throw new IllegalStateException("Unable to instantiate driver", e);
		}
	}

	private Object pickArgument(ParameterSpec p, BoskInfo<?> boskInfo, BoskDriver downstream) {
		var parameterClass = rawClass(p.type());
		if (parameterClass == BoskInfo.class) {
			return boskInfo;
		} else if (parameterClass == BoskDriver.class) {
			return downstream;
		} else {
			throw new AssertionError("Parameter types should already have been validated");
		}
	}

	private static final MethodHandle ACCEPT_DRIVER, ACCEPT_BOSK_INFO;
	private static final List<ParameterSpec> ACCEPT_DRIVER_PARAMETERS, ACCEPT_BOSK_INFO_PARAMETERS;

	static {
		try {
			Method acceptDriver = DriverAcceptingFunction.class.getDeclaredMethod("accept", BoskDriver.class);
			ACCEPT_DRIVER = MethodHandles.publicLookup().unreflect(acceptDriver);
			ACCEPT_DRIVER_PARAMETERS = Stream.of(acceptDriver.getParameters()).map(ParameterSpec::of).toList();

			Method acceptBosk = BoskInfoAcceptingFunction.class.getDeclaredMethod("accept", BoskInfo.class);
			ACCEPT_BOSK_INFO = MethodHandles.publicLookup().unreflect(acceptBosk);
			ACCEPT_BOSK_INFO_PARAMETERS = Stream.of(acceptBosk.getParameters()).map(ParameterSpec::of).toList();
		} catch (NoSuchMethodException | IllegalAccessException e) {
			throw new AssertionError(e);
		}
	}
}
