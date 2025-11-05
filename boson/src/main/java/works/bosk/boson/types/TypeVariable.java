package works.bosk.boson.types;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * Note that we don't turn the bounds into {@link DataType}s eagerly,
 * because the bounds on type variables can be self-referential,
 * and this would lead to infinite recursion.
 * Wildcards don't have this problem, so their bounds are eagerly converted.
 * <p>
 * When processing the bounds, you can always convert them to {@link DataType}s
 * as needed, but be careful to avoid infinite recursion.
 */
public record TypeVariable(String name, List<Type> bounds) implements UnknownType {
	public static TypeVariable unbounded(String name) {
		return new TypeVariable(name, List.of());
	}

	@Override
	public String toString() {
		if (bounds.isEmpty()) {
			return name;
		} else {
			StringBuilder sb = new StringBuilder();
			sb.append(name);
			sb.append(" extends ");
			for (int i = 0; i < bounds.size(); i++) {
				if (i > 0) {
					sb.append(" & ");
				}
				sb.append(bounds.get(i).getTypeName());
			}
			return sb.toString();
		}
	}

	@Override
	public boolean isAssignableFrom(DataType other) {
		return bounds.stream().allMatch(t -> DataType.of(t).isAssignableFrom(other));
	}

	@Override
	public DataType substitute(Map<String, DataType> actualArguments) {
		DataType candidate = actualArguments.get(name);
		if (candidate != null) {
			return candidate;
		} else {
			return this;
		}
	}

	@Override
	public Map<String, DataType> bindingsFor(DataType other) {
		// This is where the rubber meets the road:
		// we're binding this variable to `other`.
		return Map.of(name, other);
	}

	@Override
	public boolean hasWildcards() {
		// The type bounds can contain wildcards, but the important thing is that
		// if this type variable were substituted with a wildcard-free type, the
		// result would be fully known, and so we return false here.
		return false;
	}
}
