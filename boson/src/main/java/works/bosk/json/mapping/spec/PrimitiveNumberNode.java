package works.bosk.json.mapping.spec;

import java.util.Map;
import works.bosk.json.types.DataType;

/**
 * Represents a JSON number as a primitive number type.
 */
public record PrimitiveNumberNode(
	Class<?> targetClass
) implements ScalarSpec {
	public PrimitiveNumberNode {
		assert PRIMITIVE_NUMBER_CLASSES.containsKey(targetClass);
	}

	public static final Map<Class<?>, Class<? extends Number>> PRIMITIVE_NUMBER_CLASSES = Map.of(
		byte.class, Byte.class,
		short.class, Short.class,
		int.class, Integer.class,
		long.class, Long.class,
		float.class, Float.class,
		double.class, Double.class);

	@Override
	public String toString() {
		return "Primitive:" + targetClass.getSimpleName();
	}

	public DataType.KnownType dataType() {
		return DataType.of(targetClass());
	}
}
