package works.bosk.json.types;

public record UnboundedWildcardType() implements WildcardType {
	@Override
	public String toString() {
		return "?";
	}

	@Override
	public boolean isAssignableFrom(DataType other) {
		return other instanceof KnownType;
	}
}
