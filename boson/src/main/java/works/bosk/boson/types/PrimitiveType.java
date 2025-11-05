package works.bosk.boson.types;

import java.util.Map;

public record PrimitiveType(Class<?> rawClass) implements KnownType {
	public PrimitiveType {
		assert rawClass.isPrimitive();
	}

	@Override
	public boolean isAssignableFrom(DataType other) {
		return other instanceof PrimitiveType(var otherRawClass)
			&& rawClass.isAssignableFrom(otherRawClass);
	}

	@Override
	public boolean isAssignableFromTypeArgument(DataType other) {
		return other instanceof PrimitiveType(var otherRawClass)
			&& rawClass.equals(otherRawClass);
	}

	@Override
	public PrimitiveType substitute(Map<String, DataType> actualArguments) {
		return this;
	}

	@Override
	public Map<String, DataType> bindingsFor(DataType other) {
		return Map.of();
	}

	@Override
	public boolean isFullyKnown() {
		return true;
	}

	@Override
	public boolean hasWildcards() {
		return false;
	}

	@Override
	public String toString() {
		return rawClass.getSimpleName();
	}
}
