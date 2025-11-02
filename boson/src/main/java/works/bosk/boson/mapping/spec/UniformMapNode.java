package works.bosk.boson.mapping.spec;

import java.util.Map;
import works.bosk.boson.mapping.spec.handles.ObjectAccumulator;
import works.bosk.boson.mapping.spec.handles.ObjectEmitter;
import works.bosk.boson.types.DataType;

/**
 * @param keyNode must specify a JSON <em>string</em>. Can also accept a {@link TypeRefNode}
 *                that maps to a spec that specifies a <em>string</em>.
 */
public record UniformMapNode(
	JsonValueSpec keyNode,
	JsonValueSpec valueNode,
	ObjectAccumulator accumulator,
	ObjectEmitter emitter
) implements ObjectSpec {
	public UniformMapNode {
		// TODO: How to assert that keyNode can accept strings?
		// TypeRef makes this almost impossible to check until we have a TypeMap.

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
	public DataType dataType() {
		return accumulator.resultType();
	}

	@Override
	public String briefIdentifier() {
		return "Uniform_" + dataType().leastUpperBoundClass().getSimpleName();
	}

	@Override
	public UniformMapNode substitute(Map<String, DataType> actualArguments) {
		return new UniformMapNode(
			keyNode.substitute(actualArguments),
			valueNode.substitute(actualArguments),
			accumulator.substitute(actualArguments),
			emitter.substitute(actualArguments)
		);
	}
}
