package works.bosk.json.mapping.spec;

import works.bosk.json.mapping.spec.handles.ObjectAccumulator;
import works.bosk.json.mapping.spec.handles.ObjectEmitter;
import works.bosk.json.types.DataType;

public record UniformMapNode(
	StringSpec keyNode,
	JsonValueSpec valueNode,
	ObjectAccumulator accumulator,
	ObjectEmitter emitter
) implements ObjectSpec {
	public UniformMapNode {
		assert accumulator.keyType().isAssignableFrom(keyNode.dataType()):
			"accumulator must accept keys of type " + keyNode.dataType();
		assert accumulator.valueType().isAssignableFrom(valueNode.dataType()):
			"accumulator must accept values of type " + valueNode.dataType();

		assert emitter.getKey().returnType().isAssignableFrom(keyNode.dataType()):
			"emitter must supply keys of type " + keyNode.dataType();
		assert emitter.getValue().returnType().isAssignableFrom(valueNode.dataType()):
			"emitter must supply values of type " + valueNode.dataType();
	}

	@Override
	public DataType.KnownType dataType() {
		return accumulator.resultType();
	}
}
