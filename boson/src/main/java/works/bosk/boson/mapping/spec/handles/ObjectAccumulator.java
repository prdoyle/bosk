package works.bosk.boson.mapping.spec.handles;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Map;
import java.util.stream.Gatherer;
import works.bosk.boson.mapping.spec.UniformMapNode;
import works.bosk.boson.types.BoundType;
import works.bosk.boson.types.DataType;

import static works.bosk.boson.types.DataType.VOID;

/**
 * Describes how a {@link UniformMapNode} is to be deserialized.
 * Like {@link Gatherer} but supports primitives and key-value pairs.
 *
 * <p>
 * The handles have internal consistency rules enforced by the constructor.
 * There are some additional rules the caller should follow:
 * <ul>
 *   <li>
 *     {@code integrator} must accept values of the {@link UniformMapNode#keyNode() keyNode}'s and {@link UniformMapNode#valueNode() valueNode}'s types
 *   </li>
 *   <li>
 *     {@code finisher} must return a value of the {@link UniformMapNode}'s type
 *   </li>
 * </ul>
 *
 * The "intermediate" value produced by {@code creator} can be a primitive.
 * <p>
 * If the integrator returns void, the accumulator is assumed to be updated in-place,
 * and the one returned by the creator will continue to be used.
 * Otherwise, the integrator returns the updated accumulator.
 * <p>
 * If {@code keyHandler} returns a value (rather than void), that value is passed
 * as the last parameter to {@code integrator}, after the key and value.
 *
 * @param creator    produces the initial accumulator value
 * @param keyHandler invoked with {@code (accumulator, key)} before each entry's value; if its return type is not void, the result is passed to integrator as the last argument
 * @param integrator given the accumulator, a key, a value, and optionally the keyHandler result, updates the accumulator, and returns it (or void)
 * @param finisher   given the accumulator, returns the final result
 */
public record ObjectAccumulator(
	TypedHandle creator,
	TypedHandle keyHandler,
	TypedHandle integrator,
	TypedHandle finisher
) {
	public ObjectAccumulator {
		// TODO: injection
		assert creator.parameterTypes().isEmpty();

		assert keyHandler.parameterTypes().size() == 2;
		assert keyHandler.parameterTypes().get(0).isAssignableFrom(creator.returnType());
		assert keyHandler.parameterTypes().get(1).isAssignableFrom(integrator.parameterTypes().get(1));

		if (VOID.equals(keyHandler.returnType())) {
			assert integrator.parameterTypes().size() == 3;
		} else {
			assert integrator.parameterTypes().size() == 4;
			DataType handlerResultType = integrator.parameterTypes().get(3);
			assert handlerResultType.isAssignableFrom(keyHandler.returnType());
		}
		assert integrator.parameterTypes().getFirst().isAssignableFrom(creator.returnType());
		assert VOID.equals(integrator.returnType()) || integrator.returnType().equals(creator.returnType());

		assert finisher.parameterTypes().size() == 1;
		assert finisher.parameterTypes().getFirst().isAssignableFrom(creator.returnType());
	}

	public DataType keyType() {
		return integrator.parameterTypes().get(1);
	}

	public DataType valueType() {
		return integrator.parameterTypes().get(2);
	}

	public DataType resultType() {
		return finisher.returnType();
	}

	public ObjectAccumulator substitute(Map<String, DataType> actualArguments) {
		return new ObjectAccumulator(
			creator.substitute(actualArguments),
			keyHandler.substitute(actualArguments),
			integrator.substitute(actualArguments),
			finisher.substitute(actualArguments)
		);
	}

	/**
	 * @param <T> the type of the value object representing the object in memory
	 * @param <A> the accumulator type
	 * @param <K> the map key type
	 * @param <V> the map value type
	 */
	public interface Wrangler<T,A,K,V> {
		A create();
		A integrate(A accumulator, K key, V value);
		T finish(A accumulator);
	}

	public static <T,A,K,V> ObjectAccumulator from(Wrangler<T,A,K,V> wrangler) {
		var wranglerType = (BoundType)DataType.of(wrangler.getClass());
		var resultType = wranglerType.parameterType(Wrangler.class, 0);
		var accumulatorType = wranglerType.parameterType(Wrangler.class, 1);
		var keyType = wranglerType.parameterType(Wrangler.class, 2);
		var valueType = wranglerType.parameterType(Wrangler.class, 3);

		TypedHandle noopKeyHandler = noopKeyHandler(accumulatorType, keyType);
		return new ObjectAccumulator(
			new TypedHandle(
				WRANGLER_CREATE.bindTo(wrangler)
					.asType(MethodType.methodType(accumulatorType.leastUpperBoundClass())),
				accumulatorType, List.of()
			),
			noopKeyHandler,
			new TypedHandle(
				WRANGLER_INTEGRATE.bindTo(wrangler)
					.asType(MethodType.methodType(
						accumulatorType.leastUpperBoundClass(),
						accumulatorType.leastUpperBoundClass(),
						keyType.leastUpperBoundClass(),
						valueType.leastUpperBoundClass()
					)),
				accumulatorType, List.of(accumulatorType, keyType, valueType)
			),
			new TypedHandle(
				WRANGLER_FINISH.bindTo(wrangler)
					.asType(MethodType.methodType(
						resultType.leastUpperBoundClass(),
						accumulatorType.leastUpperBoundClass()
					)),
				resultType, List.of(accumulatorType)
			)
		);
	}

	private static TypedHandle noopKeyHandler(DataType accumulatorType, DataType keyType) {
		return TypedHandles.biConsumer(accumulatorType, keyType, (a, k) -> {});
	}

	/**
	 * @param <T> the type of the value object representing the object in memory
	 * @param <A> the accumulator type
	 * @param <K> the map key type
	 * @param <V> the map value type
	 * @param <H> the type produced by {@code keyHandler} and passed as the last argument to {@code integrator}
	 */
	public interface KeyHandlingWrangler<T,A,K,V,H> {
		A create();
		H keyHandler(A accumulator, K key);
		A integrate(A accumulator, K key, V value, H handlerResult);
		T finish(A accumulator);
	}

	public static <T,A,K,V,H> ObjectAccumulator from(KeyHandlingWrangler<T,A,K,V,H> wrangler) {
		var wranglerType = (BoundType)DataType.of(wrangler.getClass());
		var resultType = wranglerType.parameterType(KeyHandlingWrangler.class, 0);
		var accumulatorType = wranglerType.parameterType(KeyHandlingWrangler.class, 1);
		var keyType = wranglerType.parameterType(KeyHandlingWrangler.class, 2);
		var valueType = wranglerType.parameterType(KeyHandlingWrangler.class, 3);
		var handlerResultType = wranglerType.parameterType(KeyHandlingWrangler.class, 4);

		return new ObjectAccumulator(
			new TypedHandle(
				KEY_HANDLING_WRANGLER_CREATE.bindTo(wrangler)
					.asType(MethodType.methodType(accumulatorType.leastUpperBoundClass())),
				accumulatorType, List.of()
			),
			new TypedHandle(
				KEY_HANDLING_WRANGLER_KEY_HANDLER.bindTo(wrangler)
					.asType(MethodType.methodType(
						handlerResultType.leastUpperBoundClass(),
						accumulatorType.leastUpperBoundClass(),
						keyType.leastUpperBoundClass()
					)),
				handlerResultType, List.of(accumulatorType, keyType)
			),
			new TypedHandle(
				KEY_HANDLING_WRANGLER_INTEGRATE.bindTo(wrangler)
					.asType(MethodType.methodType(
						accumulatorType.leastUpperBoundClass(),
						accumulatorType.leastUpperBoundClass(),
						keyType.leastUpperBoundClass(),
						valueType.leastUpperBoundClass(),
						handlerResultType.leastUpperBoundClass()
					)),
				accumulatorType, List.of(accumulatorType, keyType, valueType, handlerResultType)
			),
			new TypedHandle(
				KEY_HANDLING_WRANGLER_FINISH.bindTo(wrangler)
					.asType(MethodType.methodType(
						resultType.leastUpperBoundClass(),
						accumulatorType.leastUpperBoundClass()
					)),
				resultType, List.of(accumulatorType)
			)
		);
	}

	private static final MethodHandle WRANGLER_CREATE;
	private static final MethodHandle WRANGLER_INTEGRATE;
	private static final MethodHandle WRANGLER_FINISH;

	private static final MethodHandle KEY_HANDLING_WRANGLER_CREATE;
	private static final MethodHandle KEY_HANDLING_WRANGLER_KEY_HANDLER;
	private static final MethodHandle KEY_HANDLING_WRANGLER_INTEGRATE;
	private static final MethodHandle KEY_HANDLING_WRANGLER_FINISH;

	static {
		try {
			MethodHandles.Lookup lookup = MethodHandles.lookup();
			WRANGLER_CREATE = lookup.findVirtual(
				Wrangler.class,
				"create",
				MethodType.methodType(Object.class)
			);
			WRANGLER_INTEGRATE = lookup.findVirtual(
				Wrangler.class,
				"integrate",
				MethodType.methodType(Object.class, Object.class, Object.class, Object.class)
			);
			WRANGLER_FINISH = lookup.findVirtual(
				Wrangler.class,
				"finish",
				MethodType.methodType(Object.class, Object.class)
			);
			KEY_HANDLING_WRANGLER_CREATE = lookup.findVirtual(
				KeyHandlingWrangler.class,
				"create",
				MethodType.methodType(Object.class)
			);
			KEY_HANDLING_WRANGLER_KEY_HANDLER = lookup.findVirtual(
				KeyHandlingWrangler.class,
				"keyHandler",
				MethodType.methodType(Object.class, Object.class, Object.class)
			);
			KEY_HANDLING_WRANGLER_INTEGRATE = lookup.findVirtual(
				KeyHandlingWrangler.class,
				"integrate",
				MethodType.methodType(Object.class, Object.class, Object.class, Object.class, Object.class)
			);
			KEY_HANDLING_WRANGLER_FINISH = lookup.findVirtual(
				KeyHandlingWrangler.class,
				"finish",
				MethodType.methodType(Object.class, Object.class)
			);
		} catch (NoSuchMethodException | IllegalAccessException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	@Override
	public String toString() {
		return "{" + keyType() + ":" + valueType() + "}->" + resultType();
	}
}
