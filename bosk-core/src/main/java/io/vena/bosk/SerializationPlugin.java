package io.vena.bosk;

import io.vena.bosk.exceptions.DeserializationException;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.Value;

import static java.util.Objects.requireNonNull;

/**
 * A "Plugin", for now, is a thing that translates Bosk objects for interfacing
 * with the outside world.  One fine day, we'll think of a better name.
 *
 * <p>
 * Serialization systems are generally not good at allowing custom logic to
 * supply any context. This class works around that limitation by supplying a
 * place to put some context, maintained using {@link ThreadLocal}s, and managed
 * using the {@link DeserializationScope} auto-closeable to make sure the thread-local context
 * state is managed correctly.
 *
 * <p>
 * Generally, applications create one instance of each plugin they need.
 * Instances are thread-safe. The only case where you might want another
 * instance is if you need to perform a second, unrelated, nested
 * deserialization while one is already in progress on the same thread. It's
 * hard to think of a reason that an application would want to do this.
 *
 * @author pdoyle
 *
 */
public abstract class SerializationPlugin {
	private final ThreadLocal<DeserializationScope> currentScope = ThreadLocal.withInitial(this::outermostScope);

	public final DeserializationScope newDeserializationScope(Path newPath) {
		DeserializationScope outerScope = currentScope.get();
		DeserializationScope newScope = new NestedDeserializationScope(outerScope, newPath, outerScope.bindingEnvironment());
		currentScope.set(newScope);
		return newScope;
	}

	public final DeserializationScope newDeserializationScope(Reference<?> ref) {
		return newDeserializationScope(ref.path());
	}

	public final DeserializationScope overlayScope(BindingEnvironment env) {
		DeserializationScope outerScope = currentScope.get();
		DeserializationScope newScope = new NestedDeserializationScope(outerScope, outerScope.path(), outerScope.bindingEnvironment().overlay(env));
		currentScope.set(newScope);
		return newScope;
	}

	public final DeserializationScope innerDeserializationScope(String lastSegment) {
		DeserializationScope outerScope = currentScope.get();
		DeserializationScope newScope = new NestedDeserializationScope(outerScope, outerScope.path().then(lastSegment), outerScope.bindingEnvironment());
		currentScope.set(newScope);
		return newScope;
	}

	private DeserializationScope outermostScope() {
		return new OutermostDeserializationScope();
	}

	public static abstract class DeserializationScope implements AutoCloseable {
		DeserializationScope(){}

		public abstract Path path();
		public abstract BindingEnvironment bindingEnvironment();

		@Override public abstract void close();
	}

	private static final class OutermostDeserializationScope extends DeserializationScope {
		@Override public Path path() { return Path.empty(); }
		@Override public BindingEnvironment bindingEnvironment() { return BindingEnvironment.empty(); }

		@Override
		public void close() {
			throw new IllegalStateException("Outermost scope should never be closed");
		}
	}

	@Value
	@EqualsAndHashCode(callSuper = false)
	private class NestedDeserializationScope extends DeserializationScope {
		DeserializationScope outer;
		Path path;
		BindingEnvironment bindingEnvironment;

		@Override
		public void close() {
			currentScope.set(requireNonNull(outer));
		}
	}

	/**
	 * Turns <code>parameterValuesByName</code> into a list suitable for
	 * passing to a constructor, in the order indicated by
	 * <code>parametersByName</code>.
	 *
	 *
	 * @param parameterValuesByName values read from the input. <em>Modified by this method.</em>
	 * @param parametersByName ordered map of constructor {@link Parameter}s.
	 * @return {@link List} of parameter values to pass to the constructor, in
	 * the same order as in <code>parametersByName</code>. Missing values are
	 * supplied where possible, such as <code>Optional.empty()</code>.
	 */
	public final List<Object> parameterValueList(Class<?> nodeClass, Map<String, Object> parameterValuesByName, LinkedHashMap<String, Parameter> parametersByName, Bosk<?> bosk) {
		List<Object> parameterValues = new ArrayList<>();
		for (Entry<String, Parameter> entry: parametersByName.entrySet()) {
			String name = entry.getKey();
			Parameter parameter = entry.getValue();
			Class<?> type = parameter.getType();

			Object value = parameterValuesByName.remove(name);
			if (value == null) {
				// Field is absent in the input
				if (Optional.class.equals(type)) {
					parameterValues.add(Optional.empty());
				} else if (Phantom.class.equals(type)) {
					parameterValues.add(Phantom.empty());
				} else {
					throw new DeserializationException("Missing field: " + name);
				}
			} else {
				parameterValues.add(value);
			}
		}
		if (parameterValuesByName.size() >= 1) {
			throw new DeserializationException("Unrecognized fields: " + parameterValuesByName.keySet());
		}
		return parameterValues;
	}

}
