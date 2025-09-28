package works.bosk.json.mapping.spec;

import works.bosk.json.types.DataType;

/**
 * Represents a JSON <em>value</em> that may be <em>null</em>.
 * (Other nodes are not required to handle null values.)
 */
public record MaybeNullSpec(JsonValueSpec child) implements JsonValueSpec {
	public MaybeNullSpec {
		assert !child.dataType().rawClass().isPrimitive();
	}

	@Override
	public String toString() {
		return child + "?";
	}

	public DataType.KnownType dataType() {
		return this.child().dataType();
	}
}
