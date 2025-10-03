package works.bosk.json.types;

public record UnknownArrayType(UnknownType elementType) implements UnknownType {
	@Override
	public String toString() {
		return elementType + "[]";
	}

	@Override
	public boolean isAssignableFrom(DataType other) {
		return switch (other) {
			case ArrayType(var otherElementType) -> elementType.isAssignableFrom(otherElementType);
			default -> false;
		};
	}
}
