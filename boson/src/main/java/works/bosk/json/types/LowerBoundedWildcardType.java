package works.bosk.json.types;

public record LowerBoundedWildcardType(ParameterOrBound lowerBound) implements WildcardType {
	@Override
	public String toString() {
		return "? super " + lowerBound;
	}

	@Override
	public boolean isAssignableFrom(DataType other) {
		return other instanceof KnownType && other.isAssignableFrom(lowerBound.dataType());
	}
}
