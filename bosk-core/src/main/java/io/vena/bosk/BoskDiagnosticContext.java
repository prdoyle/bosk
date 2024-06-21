package io.vena.bosk;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.toSet;

/**
 * A thread-local set of name-value pairs that propagate all the way from
 * submission of a driver update, through all the driver layers,
 * to the execution of hooks.
 */
public final class BoskDiagnosticContext {
	private final ThreadLocal<MapValue<String>> currentAttributes = ThreadLocal.withInitial(MapValue::empty);

	public final class DiagnosticScope implements AutoCloseable {
		final MapValue<String> oldAttributes = currentAttributes.get();
		final Scope otelScope;

		DiagnosticScope(MapValue<String> attributes) {
			currentAttributes.set(attributes);
			otelScope = PROPAGATORS.getTextMapPropagator().extract(
				Context.current(),
				attributes,
				DIAGNOSTIC_ATTRIBUTE_GETTER
			).makeCurrent();
		}

		@Override
		public void close() {
			otelScope.close();
			currentAttributes.set(oldAttributes);
		}
	}

	/**
	 * @return the current thread's value of the attribute with the given <code>name</code>,
	 * or <code>null</code> if no such attribute has been defined.
	 */
	public @Nullable String getAttribute(String name) {
		return currentAttributes.get().get(name);
	}

	public @NotNull MapValue<String> getAttributes() {
		return currentAttributes.get();
	}

	/**
	 * Adds a single attribute to the current thread's diagnostic context.
	 * If the attribute already exists, it will be replaced.
	 */
	public DiagnosticScope withAttribute(String name, String value) {
		return new DiagnosticScope(currentAttributes.get().with(name, value));
	}

	/**
	 * Adds attributes to the current thread's diagnostic context.
	 * If an attribute already exists, it will be replaced.
	 */
	public DiagnosticScope withAttributes(@NotNull MapValue<String> additionalAttributes) {
		return new DiagnosticScope(currentAttributes.get().withAll(additionalAttributes));
	}

	/**
	 * Replaces all attributes in the current thread's diagnostic context.
	 * Existing attributes are removed/replaced.
	 * <p>
	 * This is intended for propagating context from one thread to another.
	 * <p>
	 * If <code>attributes</code> is null, this is a no-op, and any existing attributes on this thread are retained.
	 * If ensuring a clean set of attributes is important, pass an empty map instead of null.
	 */
	public DiagnosticScope withOnly(@Nullable MapValue<String> attributes) {
		if (attributes == null) {
			return new DiagnosticScope(currentAttributes.get());
		} else {
			return new DiagnosticScope(attributes);
		}
	}

	public DiagnosticScope withCurrentOtelContext() {
		Map<String, String> carrier = new LinkedHashMap<>();
		TextMapPropagator textMapPropagator = PROPAGATORS
			.getTextMapPropagator();
		textMapPropagator
			.inject(
				Context.current(),
				carrier,
				(map, key, value) -> {
					if (map != null) {
						map.put(OTEL_PREFIX + key, value);
					}
				});

		return withAttributes(MapValue.fromOrderedMap(carrier));
	}

	private static final ContextPropagators PROPAGATORS = GlobalOpenTelemetry.getPropagators();

	private static final TextMapGetter<MapValue<String>> DIAGNOSTIC_ATTRIBUTE_GETTER =
		new TextMapGetter<>() {
			@Override
			public Set<String> keys(MapValue<String> carrier) {
				return carrier.keySet().stream()
					.filter(k -> k.startsWith(OTEL_PREFIX))
					.map(k -> k.substring(OTEL_PREFIX.length()))
					.collect(toSet());
			}

			@Override
			public String get(MapValue<String> carrier, String key) {
				return carrier == null ? null : carrier.get(OTEL_PREFIX + key);
			}
		};

	public static final String OTEL_PREFIX = "otel.";
}
