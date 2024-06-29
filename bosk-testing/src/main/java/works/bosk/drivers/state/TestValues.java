package works.bosk.drivers.state;

import works.bosk.ListValue;
import works.bosk.MapValue;
import works.bosk.StateTreeNode;
import java.time.temporal.ChronoUnit;
import lombok.Value;
import lombok.With;
import lombok.experimental.FieldNameConstants;

import static java.time.temporal.ChronoUnit.FOREVER;

@Value
@With
@FieldNameConstants
public class TestValues implements StateTreeNode {
	String string;
	ChronoUnit chronoUnit;
	ListValue<String> list;
	MapValue<String> map;

	public static TestValues blank() {
		return new TestValues("", FOREVER, ListValue.empty(), MapValue.empty());
	}
}
