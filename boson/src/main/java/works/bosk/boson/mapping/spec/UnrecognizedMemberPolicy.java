package works.bosk.boson.mapping.spec;

import java.util.List;
import java.util.Map;
import works.bosk.boson.exceptions.JsonContentException;
import works.bosk.boson.mapping.spec.handles.ObjectAccumulator;
import works.bosk.boson.mapping.spec.handles.ObjectEmitter;
import works.bosk.boson.types.BoundType;
import works.bosk.boson.types.DataType;

public sealed interface UnrecognizedMemberPolicy {
	UnrecognizedMemberPolicy specialize(Map<String, DataType> actualArguments);

	List<DataType> finisherArguments();

	Ignore IGNORE = new Ignore();
	Disallow DISALLOW = new Disallow();

	record Ignore() implements UnrecognizedMemberPolicy {
		@Override
		public Ignore specialize(Map<String, DataType> actualArguments) {
			return this;
		}

		@Override
		public List<DataType> finisherArguments() {
			return List.of();
		}
	}

	record Disallow() implements UnrecognizedMemberPolicy {
		@Override
		public Disallow specialize(Map<String, DataType> actualArguments) {
			return this;
		}

		@Override
		public List<DataType> finisherArguments() {
			return List.of();
		}
	}

	/**
	 * @param keyNode must specify a JSON <em>string</em>. Can also accept a {@link TypeRefNode}
	 *                that maps to a spec that specifies a <em>string</em>.
	 */
	record UniformMapPolicy(
		JsonValueSpec keyNode,
		JsonValueSpec valueNode,
		ObjectAccumulator accumulator,
		ObjectEmitter emitter
	) implements UnrecognizedMemberPolicy {
		public UniformMapPolicy {
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
		public UniformMapPolicy(
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

		public UniformMapPolicy specialize(Map<String, DataType> actualArguments) {
			return new UniformMapPolicy(
				keyNode.specialize(actualArguments),
				valueNode.specialize(actualArguments),
				accumulator.substitute(actualArguments),
				emitter.substitute(actualArguments)
			);
		}

		@Override
		public List<DataType> finisherArguments() {
			return List.of(accumulator.resultType());
		}

		public interface SingletonWrangler<T,K,V> {
			K getKey(T value);
			V getValue(T value);
			T finish(K key, V value);
		}

		/**
		 * An efficient {@link UniformMapPolicy} for maps that have just one entry.
		 *
		 * @param wrangler object that knows how to extract the single key and value,
		 *                  and how to create the value object from a key and a value
		 * @param <T> the type of the value object representing the singleton map in memory
		 * @param <K> the type of the map key (which corresponds to the JSON member name)
		 * @param <V> the type of the map value (which corresponds to the JSON member value)
		 */
		public static <T,K,V> UniformMapPolicy singleton(SingletonWrangler<T,K,V> wrangler) {
			var wranglerType = (BoundType)DataType.of(wrangler.getClass());
			var resultType = wranglerType.parameterType(SingletonWrangler.class, 0);
			var keyType = wranglerType.parameterType(SingletonWrangler.class, 1);
			var valueType = wranglerType.parameterType(SingletonWrangler.class, 2);
			Map<String, DataType> actualArguments = Map.of(
				"T", resultType,
				"K", keyType,
				"V", valueType
			);

			// The "accumulator" here calls the finisher the first time
			// the integrator is called; the actual finisher does nothing
			ObjectAccumulator accumulator = ObjectAccumulator.from(new ObjectAccumulator.Wrangler<T,T,K,V>() {
				@Override
				public T create() {
					return null;
				}

				@Override
				public T integrate(T acc, K key, V value) {
					if (acc == null) {
						return wrangler.finish(key, value);
					} else {
						throw new JsonContentException("More than one entry in singleton map");
					}
				}

				@Override
				public T finish(T acc) {
					if (acc == null) {
						throw new JsonContentException("Empty singleton map");
					} else {
						return acc;
					}
				}
			}).substitute(actualArguments);

			// The "iterator" here is a kind of countdown of how many members
			// are remaining: it starts at 1 and stops at 0. The getKey
			// and getValue methods don't actually need this counter;
			// it's there just to turn the for loop into a "do once" loop.
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

			return new UniformMapPolicy(
				new TypeRefNode(keyType),
				new TypeRefNode(valueType),
				accumulator,
				emitter
			);
		}

	}
}
