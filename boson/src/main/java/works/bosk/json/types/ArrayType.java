package works.bosk.json.types;

import java.util.Map;

public record ArrayType(KnownType elementType) implements KnownType {
	@Override
	public Class<?> rawClass() {
		return elementType.rawClass().arrayType();
	}

	@Override
	public boolean isAssignableFrom(DataType other) {
		return other instanceof works.bosk.json.types.ArrayType(var otherElementType)
			&& elementType.isAssignableFrom(otherElementType);
	}

	@Override
	public ArrayType substitute(Map<String, DataType> actualArguments) {
		return new ArrayType(elementType.substitute(actualArguments));
	}

	@Override
	public String toString() {
		return elementType + "[]";
	}
}
