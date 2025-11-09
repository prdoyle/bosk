package works.bosk.boson.types;

import java.util.Map;

public record LowerBoundedWildcardType(DataType lowerBound) implements WildcardType {
	@Override
	public String toString() {
		return "? super " + lowerBound;
	}

	@Override
	public boolean isAssignableFrom(DataType other) {
		if (other instanceof LowerBoundedWildcardType(var otherLower)) {
			return otherLower.isAssignableFrom(lowerBound);
		} else {
			// For any other type, the candidate must be a supertype of the lower bound
			return other.isAssignableFrom(lowerBound);
		}
	}

	@Override
	public Class<?> leastUpperBoundClass() {
		return Object.class;
	}

	@Override
	public LowerBoundedWildcardType substitute(Map<String, DataType> actualArguments) {
		return new LowerBoundedWildcardType(lowerBound.substitute(actualArguments));
	}
}
