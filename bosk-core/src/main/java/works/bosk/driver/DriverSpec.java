package works.bosk.driver;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.stream.Stream;
import works.bosk.BoskDriver;
import works.bosk.BoskInfo;

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

	public DriverSpec withLabels(Label... labels) {
		return new DriverSpec(
			Stream.concat(
				this.labels.stream(),
				Stream.of(labels)
			).toList(),
			this.driverFactory
		);
	}

	/**
	 * Compatibility shim for {@link works.bosk.DriverFactory}.
	 */
	public DriverSpec(Collection<? extends Label> labels, works.bosk.DriverFactory<?> factory) {
		this(
			labels,
			DriverFactory.ofVirtualMethod(BUILD_METHOD, factory)
		);
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
