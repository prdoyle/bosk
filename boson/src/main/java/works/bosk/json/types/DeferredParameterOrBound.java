package works.bosk.json.types;

import java.lang.reflect.Type;

/**
 * Self-referential types can use this to avoid infinite recursion during construction.
 */
public record DeferredParameterOrBound(Type type) implements ParameterOrBound {
	@Override
	public DataType dataType() {
		return DataType.of(type);
	}

	@Override
	public String toString() {
		return type.toString();
	}
}
