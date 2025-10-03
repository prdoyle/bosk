package works.bosk.json.types;

public record UpperBoundedWildcardType(ParameterOrBound upperBound) implements WildcardType {
	@Override
	public String toString() {
		return "? extends " + upperBound;
	}

	@Override
	public boolean isAssignableFrom(DataType other) {
		return other instanceof KnownType && upperBound.dataType().isAssignableFrom(other);
	}
}
