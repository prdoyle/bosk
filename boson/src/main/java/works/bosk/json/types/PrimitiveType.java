package works.bosk.json.types;

public record PrimitiveType(Class<?> rawClass) implements KnownType {
	public PrimitiveType {
		assert rawClass.isPrimitive();
	}

	@Override
	public boolean isAssignableFrom(DataType other) {
		return other instanceof works.bosk.json.types.PrimitiveType(var otherRawClass)
			&& rawClass.isAssignableFrom(otherRawClass);
	}

	@Override
	public String toString() {
		return rawClass.getSimpleName();
	}
}
