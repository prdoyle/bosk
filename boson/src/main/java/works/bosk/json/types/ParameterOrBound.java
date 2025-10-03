package works.bosk.json.types;

sealed public interface ParameterOrBound permits DeferredParameterOrBound, SpecifiedParameterOrBound {
	DataType dataType();

	default KnownType knownType() {
		return (KnownType) dataType();
	}
}
