package works.bosk.json.types;

/**
 * Only usable for types that are not self-referential.
 */
public record SpecifiedParameterOrBound(DataType dataType) implements ParameterOrBound {
	@Override
	public String toString() {
		return dataType.toString();
	}
}
