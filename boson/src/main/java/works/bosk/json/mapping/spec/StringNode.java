package works.bosk.json.mapping.spec;

import works.bosk.json.types.DataType;
import works.bosk.json.types.KnownType;

/**
 * Represents a JSON string as a {@link String}.
 */
public record StringNode() implements ScalarSpec {
	@Override
	public String toString() {
		return "String";
	}

	public KnownType dataType() {
		return DataType.STRING;
	}
}
