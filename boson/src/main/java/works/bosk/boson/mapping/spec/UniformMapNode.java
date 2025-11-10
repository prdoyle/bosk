package works.bosk.boson.mapping.spec;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Map;
import works.bosk.boson.mapping.spec.handles.ObjectAccumulator;
import works.bosk.boson.mapping.spec.handles.ObjectEmitter;
import works.bosk.boson.mapping.spec.handles.TypedHandle;
import works.bosk.boson.types.BoundType;
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

	public interface SingletonWrangler<T,K,V> {
		K getKey(T value);
		V getValue(T value);
		T finish(K key, V value);
	}

	/**
	 * An efficient {@link UniformMapNode} for maps that have just one entry.
	 *
	 * @param wrangler object that knows how to extract the single key and value,
	 *                  and how to create the value object from a key and a value
	 * @param <T> the type of the value object representing the singleton map in memory
	 * @param <K> the type of the map key (which corresponds to the JSON member name)
	 * @param <V> the type of the map value (which corresponds to the JSON member value)
	 */
	public static <T,K,V> UniformMapNode singleton(SingletonWrangler<T,K,V> wrangler) {
		var wranglerType = (BoundType)DataType.of(wrangler.getClass());
		var resultType = wranglerType.parameterType(SingletonWrangler.class, 0);
		var keyType = wranglerType.parameterType(SingletonWrangler.class, 1);
		var valueType = wranglerType.parameterType(SingletonWrangler.class, 2);

		var wranglerDotFinish = new TypedHandle(
			SINGLETON_WRANGLER_FINISH.bindTo(wrangler)
				.asType(MethodType.methodType(
					resultType.leastUpperBoundClass(),
					keyType.leastUpperBoundClass(),
					valueType.leastUpperBoundClass()
				)),
			resultType,
			List.of(keyType, valueType)
		);

		// The "accumulator" here calls the finisher the first time
		// the integrator is called; the actual finisher does nothing
		ObjectAccumulator accumulator = new ObjectAccumulator(
			constant(resultType, null),
			new TypedHandle(
				SINGLETON_INTEGRATOR.bindTo(wranglerDotFinish)
					.asType(MethodType.methodType(
						resultType.leastUpperBoundClass(),
						resultType.leastUpperBoundClass(),
						keyType.leastUpperBoundClass(),
						valueType.leastUpperBoundClass())
					),
				resultType,
				List.of(resultType, keyType, valueType)
			),
			new TypedHandle(
				SINGLETON_FINISHER.asType(MethodType.methodType(
					resultType.leastUpperBoundClass(),
					resultType.leastUpperBoundClass())
				),
				resultType, List.of(resultType)
			)
		);

		// The "iterator" here is a kind of countdown of how many members
		// are remaining: it starts at 1 and stops at 0. The getKey
		// and getValue methods don't actually need this counter;
		// it's there just to turn the for loop into a "do once" loop.
		ObjectEmitter emitter = new ObjectEmitter(
			constant(INT, 1).dropArguments(0, resultType),
			notEquals(constant(INT, 0)),
			constant(INT, 0).dropArguments(0, INT, resultType), // Must accept the original object engage "for-loop" form
			new TypedHandle(
				SINGLETON_WRANGLER_GET_KEY
					.bindTo(wrangler)
					.asType(
						MethodType.methodType(
							keyType.leastUpperBoundClass(),
							resultType.leastUpperBoundClass()
						)
					),
				keyType, List.of(resultType)
			).dropArguments(0, INT),
			new TypedHandle(
				SINGLETON_WRANGLER_GET_VALUE
					.bindTo(wrangler)
					.asType(
						MethodType.methodType(
							valueType.leastUpperBoundClass(),
							resultType.leastUpperBoundClass()
						)
					),
				valueType, List.of(resultType)
			).dropArguments(0, INT)
		);

		return new UniformMapNode(
			new TypeRefNode(keyType),
			new TypeRefNode(valueType),
			accumulator,
			emitter
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
	private static final MethodHandle SINGLETON_WRANGLER_GET_KEY;
	private static final MethodHandle SINGLETON_WRANGLER_GET_VALUE;
	private static final MethodHandle SINGLETON_WRANGLER_FINISH;

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
			SINGLETON_WRANGLER_GET_KEY = MethodHandles.lookup().findVirtual(SingletonWrangler.class,
				"getKey",
				MethodType.methodType(Object.class, Object.class)
			);
			SINGLETON_WRANGLER_GET_VALUE = MethodHandles.lookup().findVirtual(SingletonWrangler.class,
				"getValue",
				MethodType.methodType(Object.class, Object.class)
			);
			SINGLETON_WRANGLER_FINISH = MethodHandles.lookup().findVirtual(SingletonWrangler.class,
				"finish",
				MethodType.methodType(Object.class, Object.class, Object.class)
			);
		} catch (NoSuchMethodException | IllegalAccessException e) {
			throw new ExceptionInInitializerError(e);
		}
	}
}
