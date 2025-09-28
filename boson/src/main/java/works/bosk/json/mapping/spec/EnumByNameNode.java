package works.bosk.json.mapping.spec;

import works.bosk.json.types.DataType;

/**
 * Represents a JSON string as an enum value with the corresponding name.
 */
public record EnumByNameNode(
	Class<? extends Enum<?>> enumType
) implements StringSpec {
	@SuppressWarnings("unchecked")
	public static EnumByNameNode of(Class<?> enumType) {
		assert Enum.class.isAssignableFrom(enumType);
		return new EnumByNameNode((Class<? extends Enum<?>>) enumType);
	}

	@Override
	public String toString() {
		return "Enum:" + enumType.getSimpleName();
	}

	public DataType.KnownType dataType() {
		return DataType.of(enumType());
	}
}
