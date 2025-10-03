package works.bosk.json.types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 * An {@link InstanceType} accompanied by generic type information.
 * <p>
 * The parameters could be {@link UnknownType}s, so this doesn't
 * necessarily mean it's a fully known type.
 * <p>
 * For ordinary classes, {@code typeArguments} will be empty,
 * indicating that the class has no type parameters.
 */
public record BoundType(Class<?> rawClass, List<? extends ParameterOrBound> bindings) implements InstanceType {
	DataType typeArgument(int index) {
		return bindings().get(index).dataType();
	}

	public Stream<DataType> typeArguments() {
		return this.bindings().stream().map(ParameterOrBound::dataType);
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
			case works.bosk.json.types.BoundType bt -> isAssignableFrom(bt);
			case ErasedType _ -> true; // Seems aggressive, but people use erased types when they don't want to think about generics
		};

	}

	private boolean isAssignableFrom(works.bosk.json.types.BoundType candidate) {
		// Collect all type variable bindings.
		Map<String, ParameterOrBound> typeVariableBindings = new HashMap<>();
		for (int i = 0; i < bindings().size(); i++) {
			DataType patternArg = typeArgument(i);
			var candidateParameter = candidate.parameterBinding(rawClass(), i);
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
					when (upperBound.dataType() instanceof TypeVariable tv) -> {
					resolvedArguments.add(new UpperBoundedWildcardType(
						typeVariableBindings.getOrDefault(tv.name(), upperBound)
					));
				}
				case LowerBoundedWildcardType(var lowerBound)
					when (lowerBound.dataType() instanceof TypeVariable tv) -> {
					resolvedArguments.add(new LowerBoundedWildcardType(
						typeVariableBindings.getOrDefault(tv.name(), lowerBound)
					));
				}
				case TypeVariable(var name, var bounds) -> {
					TypeVariable resolved = new TypeVariable(name, bounds.stream().map(b -> {
						if (b.dataType() instanceof TypeVariable tv) {
							return typeVariableBindings.getOrDefault(tv.name(), b);
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
			case UnboundedWildcardType _ -> true;
			case UpperBoundedWildcardType(ParameterOrBound upperBound) -> upperBound.dataType().isAssignableFrom(candidate);
			case LowerBoundedWildcardType(ParameterOrBound lowerBound) -> candidate.isAssignableFrom(lowerBound.dataType());
			case TypeVariable tv -> tv.upperBounds().stream().allMatch(bound -> bound.dataType().isAssignableFrom(candidate));
			default -> patternArg.equals(candidate); // Generics are neither covariant nor contravariant
		};

	}

	@Override
	public String toString() {
		if (this.bindings().isEmpty()) {
			return this.rawClass().getSimpleName();
		} else {
			return this.rawClass().getSimpleName() + "<"
				+ this.bindings().stream()
				.map(ParameterOrBound::toString)
				.collect(joining(","))
				+ ">";
		}
	}
}
