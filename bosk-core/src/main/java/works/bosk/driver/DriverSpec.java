package works.bosk.driver;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import works.bosk.BoskDriver;
import works.bosk.BoskInfo;

import static works.bosk.ReferenceUtils.rawClass;

public record DriverSpec (
	Collection<? extends Label> labels,
	DriverFactory driverFactory
) {

	public DriverSpec {
		for (var label: labels) {
			if (!label.driverClass().isAssignableFrom(driverFactory.returnClass())) {
				throw new IllegalArgumentException(
					"Driver class "
					+ driverFactory.returnClass().getSimpleName()
					+ " does not conform to type "
					+ label.driverClass().getSimpleName()
					+ " of label: " + label);
			}
		}
	}

	public DriverSpec(Collection<? extends Label> labels, works.bosk.DriverFactory<?> factory) {
		this(
			labels,
			DriverFactory.ofVirtualMethod(BUILD_METHOD, factory)
		);
	}

	BoskDriver build(BoskInfo<?> boskInfo, BoskDriver downstream) {
		List<Object> args = driverFactory.parameters().stream()
			.map(t -> pickArgument(t, boskInfo, downstream))
			.toList();
		try {
			return (BoskDriver) driverFactory.methodHandle().invokeWithArguments(args);
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

	private static final Method BUILD_METHOD;

	static {
		try {
			BUILD_METHOD = works.bosk.DriverFactory.class.getDeclaredMethod("build", BoskInfo.class, BoskDriver.class);
		} catch (NoSuchMethodException e) {
			throw new IllegalStateException(e);
		}
	}
}
