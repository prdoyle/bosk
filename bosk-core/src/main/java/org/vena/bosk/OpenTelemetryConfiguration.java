package org.vena.bosk;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.TracerProvider;

/**
 * Note: this is not part of the supported public API. It's meant to allow internal
 * Bosk components to coordinate their support for OpenTelemetry.
 */
public class OpenTelemetryConfiguration {
	/**
	 * <em>Usage note</em>
	 *
	 * Our primary purpose for supporting OpenTelemetry is to help users understand
	 * their own code, not to diagnose performance problems in the Bosk library.
	 * To that end, we will do such things as create a {@link Span} when calling
	 * user-supplied code, and propagate context in a helpful manner; but think twice
	 * about adding self-serving instrumentation that doesn't help our users' troubleshooting.
	 */
	private static Tracer tracer;

	static {
		setTracerFrom(GlobalOpenTelemetry.getTracerProvider());
	}

	public static Tracer tracer() {
		return tracer;
	}

	public static synchronized void setTracerFrom(TracerProvider provider) {
		tracer = provider.get("bosk", libraryVersion());
	}

	private static String libraryVersion() {
		String result = Bosk.class.getPackage().getImplementationVersion();
		if (result == null) {
			return "dev";
		} else {
			return result;
		}
	}

}
