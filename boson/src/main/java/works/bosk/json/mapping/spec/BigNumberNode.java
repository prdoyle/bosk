package works.bosk.json.mapping.spec;

import java.math.BigDecimal;
import works.bosk.json.types.DataType;

/**
 * Represents a JSON number as a {@link Number}
 * that preserves the full value of the number.
 */
public record BigNumberNode(
	Class<? extends Number> numberClass
) implements ScalarSpec {
	public BigNumberNode {
		assert numberClass == BigDecimal.class: "Only BigDecimal is supported";
	}

	@Override
	public String toString() {
		return "BigNumber:" + numberClass.getSimpleName();
	}

	public DataType.KnownType dataType() {
		return DataType.of(numberClass());
	}
}
