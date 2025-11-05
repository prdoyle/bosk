package works.bosk.boson.types;

import java.util.Map;

public record ArrayType(KnownType elementType) implements KnownType {
	@Override
	public Class<?> rawClass() {
		return elementType.rawClass().arrayType();
	}

	@Override
	public boolean isAssignableFrom(DataType other) {
		return other instanceof works.bosk.boson.types.ArrayType(var otherElementType)
			&& elementType.isAssignableFrom(otherElementType);
	}

	@Override
	public ArrayType substitute(Map<String, DataType> actualArguments) {
		return new ArrayType(elementType.substitute(actualArguments));
	}

	@Override
	public boolean isFullyKnown() {
		return elementType.isFullyKnown();
	}

	@Override
	public boolean hasWildcards() {
		return elementType.hasWildcards();
	}

	@Override
	public Map<String, DataType> bindingsFor(DataType other) {
		assert this.isAssignableFrom(other);
		return switch (other) {
			case ArrayType(var e) -> elementType.bindingsFor(e);
			default -> throw new IllegalArgumentException("wat");
		};
	}

	@Override
	public String toString() {
		return elementType + "[]";
	}
}
