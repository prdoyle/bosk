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
			return ofParameterized(parameterizedType);
		} else if (type instanceof java.lang.reflect.TypeVariable<?> typeVariable) {
			return ofVariable(typeVariable);
		} else if (type instanceof java.lang.reflect.WildcardType wildcardType) {
			return ofWildcard(wildcardType);
		} else if (type instanceof GenericArrayType t) {
			var elementType = DataType.of(t.getGenericComponentType());
			return switch (elementType) {
				case KnownType e -> new ArrayType(e);
				case UnknownType e -> new UnknownArrayType(e);
			};
		}
		throw new IllegalArgumentException("Unsupported type: " + type);
	}

	static DataType ofWildcard(java.lang.reflect.WildcardType wildcardType) {
		if (wildcardType.getLowerBounds().length == 1) {
			return new LowerBoundedWildcardType(DataType.of(wildcardType.getLowerBounds()[0]));
		} else if (wildcardType.getUpperBounds().length == 1
			&& !wildcardType.getUpperBounds()[0].equals(Object.class)) {
			return new UpperBoundedWildcardType(DataType.of(wildcardType.getUpperBounds()[0]));
		} else {
			assert wildcardType.getLowerBounds().length == 0 && wildcardType.getUpperBounds().length == 0;
			return new UnboundedWildcardType();
		}
	}

	static TypeVariable ofVariable(java.lang.reflect.TypeVariable<?> typeVariable) {
		return new TypeVariable(typeVariable.getName(),
			Stream.of(typeVariable.getBounds())
				.map(DataType::of)
				.toList());
	}

	static BoundType ofParameterized(ParameterizedType parameterizedType) {
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
	}

	boolean isAssignableFrom(KnownType other);

	/**
	 * A {@link DataType} whose class is not known at compile time,
	 * such as a {@link TypeVariable}.
	 * <p>
	 * In general, we have little use for these, so we don't keep rich
	 * information about them.
	 * In the context of high-performance JSON processing,
	 * they should be avoided.
	 */
	sealed interface UnknownType extends DataType { }

	record PrimitiveType(Class<?> rawClass) implements KnownType {
		public PrimitiveType {
			assert rawClass.isPrimitive();
		}

		@Override
		public boolean isAssignableFrom(KnownType other) {
			return rawClass.isAssignableFrom(other.rawClass());
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
		public boolean isAssignableFrom(KnownType other) {
			return other instanceof ArrayType(var otherElementType)
				&& elementType.isAssignableFrom(otherElementType);
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

		@Override
		public boolean isAssignableFrom(KnownType other) {
			return switch (other) {
				case ArrayType(var otherElementType) -> elementType.isAssignableFrom(otherElementType);
				default -> false;
			};
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
					case ErasedType r -> DataType.ofVariable(r.rawClass().getTypeParameters()[parameterIndex]);
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

				if (candidate instanceof TypeVariable(var tvName, _)) {
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

		@Override
		public boolean isAssignableFrom(KnownType other) {
			// More lax than most isAssignableFrom implementations.
			// Erased types are used when the user wants to ignore generic type parameters.
			return rawClass.isAssignableFrom(other.rawClass());
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

		@Override
		public boolean isAssignableFrom(KnownType candidate) {
			if (!rawClass.isAssignableFrom(candidate.rawClass())) {
				return false;
			}

			return switch (candidate) {
				case ArrayType _ -> Object.class.equals(rawClass);
				case PrimitiveType _ -> false; // No instance type matches any primitive
				case ErasedType _ -> true; // TODO: is this too permissive?
				case BoundType bt -> isAssignableFrom(bt);
			};

		}

		private boolean isAssignableFrom(BoundType candidate) {
			// Step 2 — Check type argument arity
			List<DataType> candidateParams = candidate.typeArguments();
			if (typeArguments.size() != candidateParams.size()) {
				return false; // Arity mismatch
			}

			// Step 3 — Check each type argument according to containment rules
			for (int i = 0; i < typeArguments.size(); i++) {
				DataType patternArg = typeArguments.get(i);
				DataType candidateArg = candidateParams.get(i);

				if (!isAssignableTypeArgument(patternArg, candidateArg)) {
					return false;
				}
			}

			return true;
		}

		private boolean isAssignableTypeArgument(DataType patternArg, DataType candidateType) {
			// Exact equality is trivially assignable
			if (patternArg.equals(candidateType)) {
				return true;
			}

			if (!(candidateType instanceof KnownType candidate)) {
				// If the candidate isn't a known type, we're bailing out
				return false;
			}

			// Wildcard upper bound: candidate must be assignable to the bound
			if (patternArg instanceof UpperBoundedWildcardType(KnownType upperBound)) {
				return candidate.isAssignableFrom(upperBound);
			}

			// Wildcard lower bound: bound must be assignable to candidate
			if (patternArg instanceof LowerBoundedWildcardType(KnownType lowerBound)) {
				return lowerBound.isAssignableFrom(candidate);
			}

			// Type variable: candidate must satisfy all upper bounds
			if (patternArg instanceof TypeVariable tv) {
				return tv.upperBounds().stream()
					.allMatch(bound -> bound.isAssignableFrom(candidate));
			}

			// If the patterns are themselves parameterized,
			// recurse to handle those.
			return patternArg.isAssignableFrom(candidate);
		}

	}

	record TypeVariable(String name, List<DataType> upperBounds) implements UnknownType {
		@Override
		public String toString() {
			return "'" + name + "'";
		}

		@Override
		public boolean isAssignableFrom(KnownType other) {
			return upperBounds.stream().allMatch(bound -> bound.isAssignableFrom(other));
		}
	}

	sealed interface WildcardType extends UnknownType { }

	record UnboundedWildcardType() implements WildcardType {
		@Override
		public String toString() {
			return "?";
		}

		@Override
		public boolean isAssignableFrom(KnownType other) {
			return true;
		}
	}

	record UpperBoundedWildcardType(DataType upperBound) implements WildcardType {
		@Override
		public String toString() {
			return "? extends " + upperBound;
		}

		@Override
		public boolean isAssignableFrom(KnownType other) {
			return upperBound.isAssignableFrom(other);
		}
	}

	record LowerBoundedWildcardType(DataType lowerBound) implements WildcardType {
		@Override
		public String toString() {
			return "? super " + lowerBound;
		}

		@Override
		public boolean isAssignableFrom(KnownType other) {
			return switch (lowerBound) {
				case KnownType k -> k.isAssignableFrom(other);
				case TypeVariable v -> v.upperBounds().stream().allMatch(b -> b.isAssignableFrom(other));
				case WildcardType _ -> false; // Can't make sense of "? super ?"
				case UnknownArrayType _ -> false; // Can't make sense of "? super T[]" for now
			};
		}
	}
}
