package works.bosk.json.mapping.spec;

import works.bosk.json.types.DataType;

/**
 * Represents a JSON string as a {@link String}.
 */
public record StringNode() implements StringSpec {
	@Override
	public String toString() {
		return "String";
	}

	public static final StringNode INSTANCE = new StringNode();

	public DataType.KnownType dataType() {
		return DataType.of(String.class);
	}
}
