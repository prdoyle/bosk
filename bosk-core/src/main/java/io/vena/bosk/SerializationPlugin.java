package io.vena.bosk;

import io.vena.bosk.exceptions.DeserializationException;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * A "Plugin", for now, is a thing that translates Bosk objects for interfacing
 * with the outside world.  One fine day, we'll think of a better name.
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
	/**
	 * Turns <code>parameterValuesByName</code> into a list suitable for
	 * passing to a constructor, in the order indicated by
	 * <code>parametersByName</code>.
	 *
	 * <p>
	 * @param parameterValuesByName values read from the input. <em>Modified by this method.</em>
	 * @param parametersByName ordered map of constructor {@link Parameter}s.
	 * @return {@link List} of parameter values to pass to the constructor, in
	 * the same order as in <code>parametersByName</code>. Missing values are
	 * supplied where possible, such as <code>Optional.empty()</code>.
	 */
	public final List<Object> parameterValueList(Map<String, Object> parameterValuesByName, LinkedHashMap<String, Parameter> parametersByName) {
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
