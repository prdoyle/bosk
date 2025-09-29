package works.bosk.json.mapping.spec;

import works.bosk.json.types.DataType;
import works.bosk.json.types.DataType.KnownType;

/**
 * Represents a JSON string as a {@link String}.
 */
public record StringNode() implements StringSpec {
	@Override
	public String toString() {
		return "String";
	}

	public KnownType dataType() {
		return DataType.STRING;
	}
}
