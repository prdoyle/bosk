package works.bosk.json.mapping.spec;

import works.bosk.json.types.DataType;
import works.bosk.json.types.KnownType;

/**
 * Represents a JSON string as an enum value with the corresponding name.
 */
public record EnumByNameNode(
	Class<? extends Enum<?>> enumType
) implements ScalarSpec {
	@SuppressWarnings("unchecked")
	public static EnumByNameNode of(Class<?> enumType) {
		assert Enum.class.isAssignableFrom(enumType);
		return new EnumByNameNode((Class<? extends Enum<?>>) enumType);
	}

	@Override
	public String toString() {
		return "Enum:" + enumType.getSimpleName();
	}

	public KnownType dataType() {
		return DataType.known(enumType());
	}
}
