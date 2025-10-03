package works.bosk.json.types;

/**
 * A {@link DataType} representing a class or interface type,
 */
sealed public interface KnownType extends DataType permits ArrayType, InstanceType, PrimitiveType {
	Class<?> rawClass();
}
