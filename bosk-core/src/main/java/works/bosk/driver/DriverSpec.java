package works.bosk.driver;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import works.bosk.BoskDriver;
import works.bosk.BoskInfo;
import works.bosk.DriverFactory;

public record DriverSpec (
	Collection<? extends Label> labels,
	MethodHandle builderHandle
) {

	public DriverSpec {
		MethodType type = builderHandle.type();
		if (!BoskDriver.class.isAssignableFrom(type.returnType())) {
			throw new IllegalArgumentException("builderHandle must return a BoskDriver: " + type.returnType());
		}
		for (var p: type.parameterList()) {
			boolean isBoskInfo = p == BoskInfo.class;
			boolean isBoskDriver = p == BoskDriver.class;
			if (!isBoskInfo && !isBoskDriver) {
				throw new IllegalArgumentException("Invalid builderHandle parameter type: " + p);
			}
		}
		for (var label: labels) {
			if (!label.driverClass().isAssignableFrom(type.returnType())) {
				throw new IllegalArgumentException(
					"Driver class "
					+ type.returnType().getSimpleName()
					+ " does not conform to type "
					+ label.driverClass().getSimpleName()
					+ " of label: " + label);
			}
		}
	}

	public DriverSpec(Collection<? extends Label> labels, DriverFactory<?> factory) {
		this(labels, BUILD_HANDLE.bindTo(factory));
	}

	BoskDriver build(BoskInfo<?> boskInfo, BoskDriver downstream) {
		List<Object> args = builderHandle.type().parameterList().stream()
			.map(t -> pickArgument(t, boskInfo, downstream))
			.toList();
		try {
			return (BoskDriver) builderHandle.invokeWithArguments(args);
		} catch (Error e) {
			// Don't mess with errors
			throw e;
		} catch (Throwable e) {
			throw new IllegalStateException("Unable to instantiate driver", e);
		}
	}

	private Object pickArgument(Class<?> t, BoskInfo<?> boskInfo, BoskDriver downstream) {
		if (t == BoskInfo.class) {
			return boskInfo;
		} else if (t == BoskDriver.class) {
			return downstream;
		} else {
			throw new AssertionError("Parameter types should already have been validated");
		}
	}

	private static final MethodHandle BUILD_HANDLE;

	static {
		try {
			Method buildMethod = DriverFactory.class.getDeclaredMethod("build", BoskInfo.class, BoskDriver.class);
			BUILD_HANDLE = MethodHandles.lookup().unreflect(buildMethod);
		} catch (NoSuchMethodException | IllegalAccessException e) {
			throw new IllegalStateException(e);
		}
	}
}
