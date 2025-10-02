package works.bosk.json.types;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import works.bosk.Catalog;
import works.bosk.Entity;

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
	BoundType STRING = (BoundType) DataType.of(String.class);
	BoundType OBJECT = (BoundType) DataType.of(Object.class);

	static KnownType known(Type type) {
		return (KnownType) of(type);
	}

	static KnownType known(TypeReference<Catalog<? extends Entity>> ref) {
		return (KnownType) of(ref);
	}

	static DataType of(Type type) {
		if (type instanceof Class<?> clazz) {
			if (clazz.isArray()) {
				var ct = of(clazz.getComponentType());
				if (ct instanceof KnownType kt) {
					return new ArrayType(kt);
				} else {
					return new UnknownArrayType((UnknownType) ct);
				}
			} else if (clazz.isPrimitive()) {
				return new PrimitiveType(clazz);
			} else if (clazz.getTypeParameters().length == 0) {
				return new BoundType(clazz, List.of());
			} else {
				return new ErasedType(clazz);
			}
		} else if (type instanceof ParameterizedType pt) {
			return new BoundType(
				(Class<?>) pt.getRawType(),
				Stream.of(pt.getActualTypeArguments()).toList());
		} else if (type instanceof java.lang.reflect.TypeVariable<?> tv) {
			return ofVariable(tv);
		} else if (type instanceof java.lang.reflect.WildcardType w) {
			return ofWildcard(w);
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
		assert wildcardType.getLowerBounds().length <= 1 && wildcardType.getUpperBounds().length <= 1;
		if (wildcardType.getLowerBounds().length == 1) {
			assert wildcardType.getUpperBounds()[0].equals(Object.class);
			return new LowerBoundedWildcardType(wildcardType.getLowerBounds()[0]);
		} else if (wildcardType.getUpperBounds()[0].equals(Object.class)) {
			return new UnboundedWildcardType();
		} else {
			return new UpperBoundedWildcardType(wildcardType.getUpperBounds()[0]);
		}
	}

	static TypeVariable ofVariable(java.lang.reflect.TypeVariable<?> typeVariable) {
		if (typeVariable.getBounds().length == 1 && typeVariable.getBounds()[0].equals(Object.class)) {
			return new TypeVariable(typeVariable.getName(), List.of());
		} else {
			return new TypeVariable(typeVariable.getName(), Stream.of(typeVariable.getBounds()).toList());
		}
	}

	/**
	 * @return a version of this {@link DataType} with all occurrences of {@code from}
	 * replaced with {@code to}.
	 */
	default DataType substitute(Type from, Type to) {
		return this.equals(DataType.of(from))
			? DataType.of(to)
			: this;
	}

	/**
	 * Returns true if the given type is described by a {@link DataType}
	 * containing known {@link UnknownType}s.
	 */
	private static boolean isFullyDeeplyKnown(Type type, Set<Type> memo) {
		if (memo.add(type)) {
			return switch (type) {
				case Class<?> clazz -> {
					if (clazz.isArray()) {
						yield isFullyDeeplyKnown(clazz.getComponentType(), memo);
					} else if (clazz.isPrimitive()) {
						yield true;
					} else {
						// If this class has type parameters, then it has been erased,
						// so it's unknown.
						yield clazz.getTypeParameters().length == 0;
					}
				}
				case ParameterizedType pt -> Stream.of(pt.getActualTypeArguments())
					.allMatch(arg -> isFullyDeeplyKnown(arg, memo));
				default -> false;
			};
		} else {
			// This is the case where we hit a cycle before we hit a known type.
			// We'd quit if we ever saw something unknown.
			// If we're in a cycle, there's no need to sound the alarm yet.
			// We will report the problem when we encounter it.
			return true;
		}
	}

	static DataType of(TypeReference<?> ref) {
		return of(ref.reflectionType());
	}

	boolean isAssignableFrom(DataType other);

	default boolean isAssignableFrom(Type type) {
		return isAssignableFrom(DataType.of(type));
	}

	default boolean isAssignableFrom(TypeReference<?> ref) {
		return isAssignableFrom(DataType.of(ref));
	}

	/**
	 * A {@link DataType} with at least one wildcard or type variable.
	 * Also includes {@link ErasedType}.
	 */
	sealed interface UnknownType extends DataType { }

	record PrimitiveType(Class<?> rawClass) implements KnownType {
		public PrimitiveType {
			assert rawClass.isPrimitive();
		}

		@Override
		public boolean isAssignableFrom(DataType other) {
			return other instanceof PrimitiveType(var otherRawClass)
				&& rawClass.isAssignableFrom(otherRawClass);
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
		public DataType substitute(Type from, Type to) {
			DataType fromDT = DataType.of(from);
			return this.equals(fromDT)? DataType.of(to)
				: elementType.equals(fromDT) ? new ArrayType((KnownType) DataType.of(to))
				: this;
		}

		@Override
		public boolean isAssignableFrom(DataType other) {
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
		public DataType substitute(Type from, Type to) {
			DataType fromDT = DataType.of(from);
			return this.equals(fromDT)? DataType.of(to)
				: elementType.equals(fromDT) ? new ArrayType((KnownType) DataType.of(to))
				: this;
		}

		@Override
		public boolean isAssignableFrom(DataType other) {
			return switch (other) {
				case ArrayType(var otherElementType) -> elementType.isAssignableFrom(otherElementType);
				default -> false;
			};
		}
	}

	/**
	 * A {@link DataType} representing a class or interface type,
	 */
	sealed interface KnownType extends DataType {
		Class<?> rawClass();
	}

	/**
	 * Represents a class or interface type.
	 */
	sealed interface InstanceType extends KnownType {
		/**
		 * @return the type of the generic parameter of {@code targetClass} used by this type.
		 * For example, if this type inherits (directly or indirectly) {@code List<String>),
		 * then calling {@code parameterType(List.class, 0)} will return {@code String.class}.
		 */
		default DataType parameterType(Class<?> targetClass, int parameterIndex) {
			assert targetClass.isAssignableFrom(this.rawClass());
			return DataType.of(parameterBinding(targetClass, parameterIndex));
		}

		/**
		 * Like {@link #parameterType(Class, int)} but returns the {@link Type} directly.
		 */
		default Type parameterBinding(Class<?> targetClass, int parameterIndex) {
			assert targetClass.isAssignableFrom(this.rawClass());
			if (targetClass.equals(this.rawClass())) {
				return switch (this) {
					case BoundType g -> g.bindings.get(parameterIndex);
					case ErasedType r -> r.rawClass().getTypeParameters()[parameterIndex];
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
				var candidate = immediateSuperType.parameterBinding(targetClass, parameterIndex);

				if (candidate instanceof java.lang.reflect.TypeVariable<?> tv) {
					// Now the candidate type would be V, in which case
					// we want to map that to String.
					var typeParameters = rawClass().getTypeParameters();
					for (int i = 0; i < typeParameters.length; i++) {
						if (typeParameters[i].getName().equals(tv.getName())) {
							return parameterBinding(rawClass(), i);
						}
					}
					throw new IllegalStateException("Type variable " + tv.getName() + " not found in " + targetClass);
				} else {
					return candidate;
				}
			}
		}
	}
	/**
	 * A {@link KnownType} with at least some type information erased.
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
		public boolean isAssignableFrom(DataType other) {
			// More lax than most isAssignableFrom implementations.
			// Erased types are used when the user wants to ignore generic type parameters.
			return other instanceof KnownType k && rawClass.isAssignableFrom(k.rawClass());
		}
	}

	/**
	 * An {@link InstanceType} accompanied by generic type information.
	 * <p>
	 * The parameters could be {@link UnknownType}s, so this doesn't
	 * necessarily mean it's a fully known type.
	 * <p>
	 * For ordinary classes, {@code typeArguments} will be empty,
	 * indicating that the class has no type parameters.
	 */
	record BoundType(Class<?> rawClass, List<Type> bindings) implements InstanceType {
		DataType typeArgument(int index) {
			return DataType.of(bindings().get(index));
		}

		public Stream<DataType> typeArguments() {
			return this.bindings().stream().map(DataType::of);
		}

		@Override
		public DataType substitute(Type from, Type to) {
			DataType fromDT = DataType.of(from);
			return this.equals(fromDT)? DataType.of(to)
				: new BoundType(rawClass, bindings.stream().map(b -> (b.equals(from) ? to : b)).toList());
		}

		public boolean isAssignableFrom(DataType candidateType) {
			if (!(candidateType instanceof KnownType candidate)) {
				// Known types can't be assignable from unknown ones
				return false;
			}
			if (!rawClass().isAssignableFrom(candidate.rawClass())) {
				return false;
			}

			return switch (candidate) {
				case ArrayType _ -> Object.class.equals(rawClass());
				case PrimitiveType _ -> false; // No instance type matches any primitive
				case BoundType bt -> isAssignableFrom(bt);
				case ErasedType _ -> true; // Seems aggressive, but people use erased types when they don't want to think about generics
			};

		}

		private boolean isAssignableFrom(BoundType candidate) {
			// Collect all type variable bindings.
			Map<String, Type> typeVariableBindings = new HashMap<>();
			for (int i = 0; i < bindings().size(); i++) {
				DataType patternArg = typeArgument(i);
				Type candidateParameter = candidate.parameterBinding(rawClass(), i);
				if (patternArg instanceof TypeVariable tv) {
					var existing = typeVariableBindings.put(tv.name(), candidateParameter);
					if (existing != null && !existing.equals(candidateParameter)) {
						// Conflicting bindings for the same type variable
						return false;
					}
				}
			}

			// For each type argument with bounds that are themselves type variables,
			// substitute the values of those bindings.
			List<DataType> resolvedArguments = new ArrayList<>();
			typeArguments().forEach(arg -> {
				switch (arg) {
					case UpperBoundedWildcardType(var upperBound)
						when (upperBound instanceof java.lang.reflect.TypeVariable<?> tv) -> {
						resolvedArguments.add(new UpperBoundedWildcardType(
							typeVariableBindings.getOrDefault(tv.getName(), upperBound)
						));
					}
					case LowerBoundedWildcardType( var lowerBound)
						when (lowerBound instanceof java.lang.reflect.TypeVariable<?> tv) -> {
						resolvedArguments.add(new LowerBoundedWildcardType(
							typeVariableBindings.getOrDefault(tv.getName(), lowerBound)
						));
					}
					case TypeVariable(var name, var bounds) -> {
						TypeVariable resolved = new TypeVariable(name, bounds.stream().map(b -> {
							if (b instanceof java.lang.reflect.TypeVariable<?> tv) {
								return typeVariableBindings.getOrDefault(tv.getName(), b);
							} else {
								return b;
							}
						}).toList());
						resolvedArguments.add(resolved);
					}
					default -> resolvedArguments.add(arg);
				}
			});

			for (int i = 0; i < resolvedArguments.size(); i++) {
				DataType patternArg = resolvedArguments.get(i);
				DataType candidateParameter = candidate.parameterType(rawClass(), i);
				if (!isAssignableTypeArgument(patternArg, candidateParameter)) {
					return false;
				}
			}

			return true;
		}

		private boolean isAssignableTypeArgument(DataType patternArg, DataType candidateType) {
			if (patternArg.equals(candidateType)) {
				// Trivially assignable
				return true;
			}

			if (!(candidateType instanceof KnownType candidate)) {
				// If the candidate isn't a known type, we're bailing out
				return false;
			}

			return switch (patternArg) {
				case UnboundedWildcardType _ ->
					true;
				case UpperBoundedWildcardType(Type upperBound) ->
					DataType.of(upperBound).isAssignableFrom(candidate);
				case LowerBoundedWildcardType(Type lowerBound) ->
					candidate.isAssignableFrom(lowerBound);
				case TypeVariable tv ->
					tv.upperBounds().stream().allMatch(bound -> DataType.of(bound).isAssignableFrom(candidate));
				default ->
					patternArg.equals(candidate); // Generics are neither covariant nor contravariant
			};

		}

		@Override
		public String toString() {
			if (this.bindings().isEmpty()) {
				return this.rawClass().getSimpleName();
			} else {
				return this.rawClass().getSimpleName() + "<"
					+ this.bindings().stream()
						.map(Type::toString)
						.collect(joining(","))
					+ ">";
			}
		}
	}

	record TypeVariable(String name, List<Type> upperBounds) implements UnknownType {

		public TypeVariable(String name, Type...upperBounds) {
			this(name, List.of(upperBounds));
		}

		@Override
		public String toString() {
			if (upperBounds.isEmpty()) {
				return name;
			} else {
				return name + " extends "
					+ upperBounds.stream().map(Type::toString).collect(joining(" & "));
			}
		}

		@Override
		public DataType substitute(Type from, Type to) {
			DataType fromDT = DataType.of(from);
			return this.equals(fromDT)? DataType.of(to)
				: new TypeVariable(name, upperBounds.stream().map(b -> (b.equals(from) ? to : b)).toList());
		}

		@Override
		public boolean isAssignableFrom(DataType other) {
			return other instanceof KnownType
				&& upperBounds.stream().allMatch(bound -> DataType.of(bound).isAssignableFrom(other));
		}
	}

	sealed interface WildcardType extends UnknownType {
		static UnboundedWildcardType unbounded() {
			return new UnboundedWildcardType();
		}

		static UpperBoundedWildcardType extends_(Type upperBound) {
			return new UpperBoundedWildcardType(upperBound);
		}

		static LowerBoundedWildcardType super_(Type lowerBound) {
			return new LowerBoundedWildcardType(lowerBound);
		}
	}

	record UnboundedWildcardType() implements WildcardType {
		@Override
		public String toString() {
			return "?";
		}

		@Override
		public boolean isAssignableFrom(DataType other) {
			return other instanceof KnownType;
		}
	}

	record UpperBoundedWildcardType(Type upperBound) implements WildcardType {
		@Override
		public String toString() {
			return "? extends " + upperBound;
		}

		@Override
		public DataType substitute(Type from, Type to) {
			DataType fromDT = DataType.of(from);
			return this.equals(fromDT)? DataType.of(to)
				: upperBound.equals(from)? new UpperBoundedWildcardType(to)
				: this;
		}

		@Override
		public boolean isAssignableFrom(DataType other) {
			return other instanceof KnownType && DataType.of(upperBound).isAssignableFrom(other);
		}
	}

	record LowerBoundedWildcardType(Type lowerBound) implements WildcardType {
		@Override
		public String toString() {
			return "? super " + lowerBound;
		}

		@Override
		public DataType substitute(Type from, Type to) {
			DataType fromDT = DataType.of(from);
			return this.equals(fromDT)? DataType.of(to)
				: lowerBound.equals(from)? new LowerBoundedWildcardType(to)
				: this;
		}

		@Override
		public boolean isAssignableFrom(DataType other) {
			return other instanceof KnownType && other.isAssignableFrom(lowerBound);
		}
	}

}
