package works.bosk.json.types;

import java.lang.reflect.Type;

sealed public interface WildcardType extends UnknownType permits LowerBoundedWildcardType, UnboundedWildcardType, UpperBoundedWildcardType {
	static UnboundedWildcardType unbounded() {
		return new UnboundedWildcardType();
	}

	static UpperBoundedWildcardType extends_(Type upperBound) {
		return new UpperBoundedWildcardType(new DeferredParameterOrBound(upperBound));
	}

	static LowerBoundedWildcardType super_(Type lowerBound) {
		return new LowerBoundedWildcardType(new DeferredParameterOrBound(lowerBound));
	}
}
