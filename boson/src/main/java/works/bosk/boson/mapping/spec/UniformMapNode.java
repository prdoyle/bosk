package works.bosk.boson.mapping.spec;

import java.util.Map;
import works.bosk.boson.exceptions.JsonContentException;
import works.bosk.boson.mapping.spec.handles.ObjectAccumulator;
import works.bosk.boson.mapping.spec.handles.ObjectEmitter;
import works.bosk.boson.types.BoundType;
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
			"accumulator must accept keys of type " + keyNode.dataType() + ", not " + accumulator.keyType();
		assert accumulator.valueType().isAssignableFrom(valueNode.dataType()):
			"accumulator must accept values of type " + valueNode.dataType() + ", not " + accumulator.valueType();

		assert emitter.getKey().returnType().isAssignableFrom(keyNode.dataType()):
			"emitter must supply keys of type " + keyNode.dataType() + ", not " + emitter.getKey().returnType();
		assert emitter.getValue().returnType().isAssignableFrom(valueNode.dataType()):
			"emitter must supply values of type " + valueNode.dataType() + ", not " + emitter.getValue().returnType();
	}

	/**
	 * Uses {@link TypeRefNode}s for {@link #keyNode()} and {@link #valueNode()}.
	 */
	public UniformMapNode(
		ObjectAccumulator accumulator,
		ObjectEmitter emitter
	) {
		this(
			new TypeRefNode(accumulator.keyType()),
			new TypeRefNode(accumulator.valueType()),
			accumulator,
			emitter
		);
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
	public UniformMapNode specialize(Map<String, DataType> actualArguments) {
		return new UniformMapNode(
			keyNode.specialize(actualArguments),
			valueNode.specialize(actualArguments),
			accumulator.substitute(actualArguments),
			emitter.substitute(actualArguments)
		);
	}

	public interface OneMemberWrangler<T,K,V> {
		K getKey(T value);
		V getValue(T value);
		T finish(K key, V value);
	}

	/**
	 * Wrangler corresponding to a common use case for {@link ObjectAccumulator#keyHandler()}.
	 * <p>
	 * {@code beforeValue} is called after the key is parsed but before the value.
	 * {@code afterValue} is called after the value is parsed, but before
	 * {@link OneMemberWrangler#finish}.
	 *
	 * @param <K> the map key type
	 * @param <V> the map value type
	 * @param <H> the type produced by {@code beforeValue} and passed to {@code afterValue}
	 */
	public interface MemberValueWrangler<K,V,H> {
		H beforeValue(K key);
		void afterValue(K key, V value, H handlerResult);

		static <K,V,H> MemberValueWrangler<K,V,H> nop() {
			return new MemberValueWrangler<>() {
				@Override public H beforeValue(K key) { return null; }
				@Override public void afterValue(K key, V value, H handlerResult) { }
			};
		}
	}

	/**
	 * An efficient {@link UniformMapNode} for maps that have just one entry.
	 *
	 * @param wrangler object that knows how to extract the single key and value,
	 *                  and how to create the value object from a key and a value
	 * @param <T> the type of the value object representing the single-member map in memory
	 * @param <K> the type of the map key (which corresponds to the JSON member name)
	 * @param <V> the type of the map value (which corresponds to the JSON member value)
	 */
	public static <T,K,V> UniformMapNode oneMember(OneMemberWrangler<T,K,V> wrangler) {
		return oneMember(wrangler, MemberValueWrangler.nop());
	}

	/**
	 * An efficient {@link UniformMapNode} for maps that have just one entry,
	 * with a {@link MemberValueWrangler} for lifecycle hooks during deserialization.
	 *
	 * @param wrangler       knows how to extract the single key and value, and how to create the value object
	 * @param memberValueWrangler lifecycle hooks for before/after the member value
	 * @param <T> the type of the value object representing the single-member map in memory
	 * @param <K> the type of the map key (which corresponds to the JSON member name)
	 * @param <V> the type of the map value (which corresponds to the JSON member value)
	 * @param <H> the type produced by {@link MemberValueWrangler#beforeValue} and passed to {@link MemberValueWrangler#afterValue}
	 */
	public static <T,K,V,H> UniformMapNode oneMember(
		OneMemberWrangler<T,K,V> wrangler,
		MemberValueWrangler<K,V,H> memberValueWrangler
	) {
		var wranglerType = (BoundType)DataType.of(wrangler.getClass());
		var resultType = wranglerType.parameterType(OneMemberWrangler.class, 0);
		var keyType = wranglerType.parameterType(OneMemberWrangler.class, 1);
		var valueType = wranglerType.parameterType(OneMemberWrangler.class, 2);
		var memberWranglerType = (BoundType)DataType.of(memberValueWrangler.getClass());
		var handlerResultType = memberWranglerType.parameterType(MemberValueWrangler.class, 2);
		Map<String, DataType> actualArguments = Map.of(
			"T", resultType,
			"K", keyType,
			"V", valueType,
			"H", handlerResultType
		);

		ObjectAccumulator accumulator = ObjectAccumulator.from(
			new ObjectAccumulator.KeyHandlingWrangler<T,T,K,V,H>() {
				@Override
				public T create() {
					return null;
				}

				@Override
				public H keyHandler(T a, K key) {
					return memberValueWrangler.beforeValue(key);
				}

				@Override
				public T integrate(T a, K key, V value, H handlerResult) {
					memberValueWrangler.afterValue(key, value, handlerResult);
					if (a == null) {
						return wrangler.finish(key, value);
					} else {
						throw new JsonContentException("More than one entry in single-member map");
					}
				}

				@Override
				public T finish(T a) {
					if (a == null) {
						throw new JsonContentException("Empty single-member map");
					} else {
						return a;
					}
				}
			}
		).substitute(actualArguments);

		ObjectEmitter emitter = ObjectEmitter.forLoop(new ObjectEmitter.ForLoopWrangler<T,K,V>() {
			@Override
			public long start(T obj) {
				return 1;
			}

			@Override
			public boolean hasNext(long iter, T obj) {
				return iter > 0;
			}

			@Override
			public long next(long iter, T obj) {
				return iter - 1;
			}

			@Override
			public K getKey(long iter, T obj) {
				return wrangler.getKey(obj);
			}

			@Override
			public V getValue(long iter, T obj) {
				return wrangler.getValue(obj);
			}
		}).substitute(actualArguments);

		return new UniformMapNode(
			new TypeRefNode(keyType),
			new TypeRefNode(valueType),
			accumulator,
			emitter
		);
	}

}
