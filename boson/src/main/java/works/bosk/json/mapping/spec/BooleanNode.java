package works.bosk.json.mapping.spec;

import works.bosk.json.types.DataType;

/**
 * Represents a JSON {@code true} or {@code false} value as a {@code boolean}.
 */
public record BooleanNode() implements ScalarSpec {
	@Override
	public String toString() {
		return "boolean";
	}

	public DataType.KnownType dataType() {
		return DataType.BOOLEAN;
	}
}
