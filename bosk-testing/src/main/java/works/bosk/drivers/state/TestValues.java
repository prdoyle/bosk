package works.bosk.drivers.state;

import java.lang.reflect.Type;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import lombok.Value;
import lombok.With;
import lombok.experimental.FieldNameConstants;
import works.bosk.Identifier;
import works.bosk.ListValue;
import works.bosk.MapValue;
import works.bosk.StateTreeNode;
import works.bosk.VariantNode;
import works.bosk.annotations.VariantCaseMap;

import static java.time.temporal.ChronoUnit.FOREVER;

@Value
@With
@FieldNameConstants
public class TestValues implements StateTreeNode {
	String string;
	ChronoUnit chronoUnit;
	ListValue<String> list;
	MapValue<String> map;
	Variant variant;

	public interface Variant extends VariantNode {
		@Override
		default String tag() {
			if (this instanceof StringCase) {
				return "string";
			} else {
				return "identifier";
			}
		}

		@VariantCaseMap
		MapValue<Type> VARIANT_CASE_MAP = MapValue.copyOf(Map.of(
			"string", StringCase.class,
			"identifier", IdentifierCase.class
		));
	}
	public record StringCase(String value) implements Variant {}
	public record IdentifierCase(Identifier value) implements Variant {}

	public static TestValues blank() {
		return new TestValues("", FOREVER, ListValue.empty(), MapValue.empty(), new StringCase(""));
	}
}
