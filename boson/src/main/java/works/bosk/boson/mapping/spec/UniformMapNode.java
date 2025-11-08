package works.bosk.boson.mapping.spec;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Map;
import works.bosk.boson.mapping.spec.handles.ObjectAccumulator;
import works.bosk.boson.mapping.spec.handles.ObjectEmitter;
import works.bosk.boson.mapping.spec.handles.TypedHandle;
import works.bosk.boson.types.DataType;

import static works.bosk.boson.mapping.spec.handles.TypedHandles.constant;
import static works.bosk.boson.mapping.spec.handles.TypedHandles.notEquals;
import static works.bosk.boson.types.DataType.INT;

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
			"accumulator must accept keys of type " + keyNode.dataType() + ", not " + accumulator.keyType();
		assert accumulator.valueType().isAssignableFrom(valueNode.dataType()):
			"accumulator must accept values of type " + valueNode.dataType() + ", not " + accumulator.valueType();

		assert emitter.getKey().returnType().isAssignableFrom(keyNode.dataType()):
			"emitter must supply keys of type " + keyNode.dataType() + ", not " + emitter.getKey().returnType();
		assert emitter.getValue().returnType().isAssignableFrom(valueNode.dataType()):
			"emitter must supply values of type " + valueNode.dataType() + ", not " + emitter.getValue().returnType();
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
	 * @param getKey    given the value object, returns the key (ie. member name)
	 * @param getValue  given the value object, returns the member value
	 */
	public static UniformMapNode singleton(
		JsonValueSpec keyNode,
		JsonValueSpec valueNode,
		TypedHandle finisher,
		TypedHandle getKey,
		TypedHandle getValue
	) {
		return new UniformMapNode(
			keyNode,
			valueNode,
			new ObjectAccumulator( // The "accumulator" here calls the finisher the first time the integrator is called; the actual finisher does nothing
				constant(finisher.returnType(), null), // Hmm what about primitives?
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
					SINGLETON_FINISHER.asType(MethodType.methodType(
						finisher.returnType().leastUpperBoundClass(),
						finisher.returnType().leastUpperBoundClass())
					),
					finisher.returnType(), List.of(finisher.returnType())
				)
			),
			new ObjectEmitter( // The "iterator" here is a kind of countdown of how many members are remaining
				constant(INT, 1).dropArguments(0, finisher.returnType()),
				notEquals(constant(INT, 0)),
				constant(INT, 0).dropArguments(0, INT, finisher.returnType()), // Must accept the original object engage "for-loop" form
				getKey.dropArguments(0, INT),
				getValue.dropArguments(0, INT)
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
