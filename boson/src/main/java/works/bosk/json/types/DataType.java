package works.bosk.json.types;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public sealed interface DataType {
	KnownType VOID = new PrimitiveType(void.class);
	PrimitiveType BOOLEAN = new PrimitiveType(boolean.class);
	PrimitiveType BYTE = new PrimitiveType(byte.class);
	PrimitiveType SHORT = new PrimitiveType(short.class);
	PrimitiveType INT = new PrimitiveType(int.class);
	PrimitiveType LONG = new PrimitiveType(long.class);
	PrimitiveType FLOAT = new PrimitiveType(float.class);
	PrimitiveType DOUBLE = new PrimitiveType(double.class);
	PrimitiveType CHAR = new PrimitiveType(char.class);
	WildcardType WILDCARD = new WildcardType();
	InstanceType STRING = (InstanceType) DataType.of(String.class);
	InstanceType OBJECT = (InstanceType) DataType.of(Object.class);

	static KnownType of(Class<?> type) {
		return (KnownType) DataType.of((Type)type);
	}

	static DataType of(Type type) {
		if (type instanceof Class<?> clazz) {
			if (clazz.isArray()) {
				return new ArrayType(of(clazz.getComponentType()));
			} else if (clazz.isPrimitive()) {
				return new PrimitiveType(clazz);
			} else if (clazz.getTypeParameters().length == 0) {
				return new BoundType(clazz, List.of());
			} else {
				return new ErasedType(clazz);
			}
		} else if (type instanceof ParameterizedType parameterizedType) {
			return of(parameterizedType);
		} else if (type instanceof java.lang.reflect.TypeVariable<?> typeVariable) {
			return of(typeVariable);
		} else if (type instanceof java.lang.reflect.WildcardType) {
			return WILDCARD;
		} else if (type instanceof GenericArrayType t) {
			var elementType = DataType.of(t.getGenericComponentType());
			return switch (elementType) {
				case KnownType e -> new ArrayType(e);
				case UnknownType e -> new UnknownArrayType(e);
			};
		}
		throw new IllegalArgumentException("Unsupported type: " + type);
	}

	static TypeVariable of(java.lang.reflect.TypeVariable<?> typeVariable) {
		return new TypeVariable(typeVariable.getName());
	}

	static BoundType of(ParameterizedType parameterizedType) {
		Class<?> rawType = (Class<?>) parameterizedType.getRawType();
		List<DataType> typeArguments = Stream.of(parameterizedType.getActualTypeArguments())
			.map(DataType::of)
			.toList();
		return new BoundType(rawType, typeArguments);
	}

	static DataType of(TypeReference<?> ref) {
		return of(ref.reflectionType());
	}

	/**
	 * A {@link DataType} whose {@link #rawClass} is known.
	 */
	sealed interface KnownType extends DataType {
		Class<?> rawClass();
		default boolean isAssignableFrom(KnownType other) {
			// TODO: Handle generics
			return this.rawClass().isAssignableFrom(other.rawClass());
		}
	}

	/**
	 * A {@link DataType} whose class is not known at compile time,
	 * such as a {@link TypeVariable}.
	 * <p>
	 * In general, we have little use for these, so we don't keep rich
	 * information about them.
	 * In the context of high-performance JSON processing,
	 * they should be avoided.
	 */
	sealed interface UnknownType extends DataType {

	}

	record PrimitiveType(Class<?> rawClass) implements KnownType {
		public PrimitiveType {
			assert rawClass.isPrimitive();
		}

		@Override
		public String toString() {
			return rawClass.getSimpleName();
		}
	}

	record ArrayType(KnownType elementType) implements KnownType {
		@Override
		public Class<?> rawClass() {
			return elementType.rawClass().arrayType();
		}

		@Override
		public String toString() {
			return elementType + "[]";
		}
	}

	record UnknownArrayType(UnknownType elementType) implements UnknownType {
		@Override
		public String toString() {
			return elementType + "[]";
		}
	}

	sealed interface InstanceType extends KnownType {
		Class<?> rawClass();

		/**
		 * @return the type of the generic parameter of {@code targetClass} used by this type.
		 * For example, if this type inherits (directly or indirectly) {@code List<String>),
		 * then calling {@code parameterType(List.class, 0)} will return {@code String.class}.
		 */
		default DataType parameterType(Class<?> targetClass, int parameterIndex) {
			assert targetClass.isAssignableFrom(this.rawClass());
			if (targetClass.equals(this.rawClass())) {
				return switch (this) {
					case BoundType g -> g.typeArguments().get(parameterIndex);
					case ErasedType r -> DataType.of(r.rawClass().getTypeParameters()[parameterIndex]);
				};
			} else {
				// First, find some immediate supertype that is a subtype of targetClass.
				InstanceType immediateSuperType = null;
				if (targetClass.isInterface()) {
					for (var i: this.rawClass().getGenericInterfaces()) {
						immediateSuperType = (InstanceType) DataType.of(i);
						if (targetClass.isAssignableFrom(immediateSuperType.rawClass())) {
							break;
						} else {
							immediateSuperType = null;
						}
					}
				}
				if (immediateSuperType == null) {
					// If it's not an interface, then it must be from our superclass
					immediateSuperType = (InstanceType) DataType.of(rawClass().getGenericSuperclass());
				}

				// For an example, suppose...
				// class S extends T<String> where class T<V> extends List<V>
				// ...and we're calling S.parameterType(List.class, 0).
				// The right answer is String.

				// First, let's recurse into the superclass...
				var candidate = immediateSuperType.parameterType(targetClass, parameterIndex);

				if (candidate instanceof TypeVariable(var tvName)) {
					// Now the candidate type would be V, in which case
					// we want to map that to String.
					var typeParameters = rawClass().getTypeParameters();
					for (int i = 0; i < typeParameters.length; i++) {
						if (typeParameters[i].getName().equals(tvName)) {
							return parameterType(rawClass(), i);
						}
					}
					throw new IllegalStateException("Type variable " + tvName + " not found in " + targetClass);
				} else {
					return candidate;
				}
			}

		}
	}

	/**
	 * A {@link InstanceType} with at least some type information erased.
	 */
	record ErasedType(Class<?> rawClass) implements InstanceType {
		public ErasedType {
			assert !rawClass.isPrimitive();
			assert !rawClass.isArray();
			assert rawClass.getTypeParameters().length > 0;
		}

		@Override
		public String toString() {
			return rawClass.getSimpleName();
		}
	}

	/**
	 * A {@link InstanceType} accompanied by generic type information.
	 * <p>
	 * The parameters could be {@link UnknownType}s, so this doesn't
	 * necessarily mean it's a fully known type.
	 * <p>
	 * For ordinary classes, {@code typeArguments} will be empty,
	 * indicating that the class has no type parameters,
	 * yet we still call it a (trivial) "generic type"
	 * to distinguish it from {@link ErasedType},
	 * which asserts that there exist generic type parameters left unspecified,
	 * which would be represented as one or more {@link TypeVariable}s.
	 */
	record BoundType(Class<?> rawClass, List<DataType> typeArguments) implements InstanceType {
		@Override
		public String toString() {
			if (typeArguments.isEmpty()) {
				return rawClass.getSimpleName();
			} else {
				return rawClass.getSimpleName() + "<"
					+ typeArguments.stream().map(DataType::toString).collect(joining(","))
					+ ">";
			}
		}
	}

	record TypeVariable(String name) implements UnknownType {
		@Override
		public String toString() {
			return "'" + name + "'";
		}
	}

	record WildcardType() implements UnknownType {
		@Override
		public String toString() {
			return "*";
		}
	}

}
