package works.bosk.json.mapping.spec;

import works.bosk.json.types.DataType;
import works.bosk.json.types.KnownType;

/**
 * Represents a JSON {@code true} or {@code false} value as a {@code boolean}.
 */
public record BooleanNode() implements ScalarSpec {
	@Override
	public String toString() {
		return "boolean";
	}

	public KnownType dataType() {
		return DataType.BOOLEAN;
	}
}
