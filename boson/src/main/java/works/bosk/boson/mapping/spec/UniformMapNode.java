package works.bosk.boson.mapping.spec;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Map;
import works.bosk.boson.mapping.spec.handles.ObjectAccumulator;
import works.bosk.boson.mapping.spec.handles.ObjectEmitter;
import works.bosk.boson.mapping.spec.handles.TypedHandle;
import works.bosk.boson.mapping.spec.handles.TypedHandles;
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

	/**
	 * An efficient {@link UniformMapNode} for maps that have just one entry.
	 *
	 * @param keyNode   specification for the key
	 * @param valueNode specification for the value
	 * @param finisher  given a key and a value, returns value object
	 */
	public static UniformMapNode singleton(
		JsonValueSpec keyNode,
		JsonValueSpec valueNode,
		TypedHandle finisher
	) {
		return new UniformMapNode(
			keyNode,
			valueNode,
			new ObjectAccumulator(
				TypedHandles.constant(finisher.returnType(), null),
				new TypedHandle(
					SINGLETON_INTEGRATOR.bindTo(finisher)
						.asType(MethodType.methodType(
							finisher.returnType().leastUpperBoundClass(),
							finisher.returnType().leastUpperBoundClass(),
							keyNode.dataType().leastUpperBoundClass(),
							valueNode.dataType().leastUpperBoundClass())
						),
					finisher.returnType(),
					List.of(finisher.returnType(), keyNode.dataType(), valueNode.dataType())
				),
				new TypedHandle(
					SINGLETON_FINISHER,
					finisher.returnType(), List.of(finisher.returnType())
				)
			),
			new ObjectEmitter(
				TypedHandles.constant(DataType.INT, 1),
				TypedHandles.notEquals(TypedHandles.constant(DataType.INT, 0)),
				next,
				getKey,
				getValue
			)
		);
	}

	@SuppressWarnings("unchecked")
	private static <T> T singletonIntegrator(TypedHandle finisher, T accumulator, Object key, Object value) {
		if (accumulator == null) {
			// TODO: Shouldn't have to call invoke here. Probably should use MethodHandles.guardWithTest
			return (T) finisher.invoke(key, value);
		} else {
			throw new IllegalStateException("More than one entry in singleton map");
		}
	}

	private static <T> T singletonFinisher(T accumulator) {
		if (accumulator == null) {
			throw new IllegalStateException("Empty singleton map");
		} else {
			return accumulator;
		}
	}

	private static final MethodHandle SINGLETON_INTEGRATOR;
	private static final MethodHandle SINGLETON_FINISHER;

	static {
		try {
			SINGLETON_INTEGRATOR = MethodHandles.lookup().findStatic(UniformMapNode.class,
				"singletonIntegrator",
				MethodType.methodType(Object.class, TypedHandle.class, Object.class, Object.class, Object.class)
			);
			SINGLETON_FINISHER = MethodHandles.lookup().findStatic(UniformMapNode.class,
				"singletonFinisher",
				MethodType.methodType(Object.class, Object.class)
			);
		} catch (NoSuchMethodException | IllegalAccessException e) {
			throw new ExceptionInInitializerError(e);
		}
	}
}
