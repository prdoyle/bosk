package works.bosk.boson.types;

/**
 * A {@link DataType} with at least one wildcard or type variable.
 * Also includes {@link ErasedType}.
 */
sealed public interface UnknownType extends DataType permits TypeVariable, UnknownArrayType, WildcardType {
	default boolean isFullyKnown() {
		return false;
	}

	@Override
	default boolean isAssignableFromTypeArgument(DataType other) {
		// All the unknown types are already neither covariant nor contravariant,
		// except in their type bounds, so isAssignableFrom works just the way we want.
		return isAssignableFrom(other);
	}
}
