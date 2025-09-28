package works.bosk.json.mapping.spec;

import works.bosk.json.types.DataType;

import static works.bosk.json.mapping.spec.PrimitiveNumberNode.PRIMITIVE_NUMBER_CLASSES;

public record BoxedPrimitiveSpec(PrimitiveNumberNode child) implements ScalarSpec {
	@Override
	public String toString() {
		return "Boxed(" + child + ")";
	}

	@Override
	public DataType.KnownType dataType() {
		return DataType.of(targetClass());
	}

	public Class<? extends Number> targetClass() {
		return PRIMITIVE_NUMBER_CLASSES.get(child.targetClass());
	}
}
