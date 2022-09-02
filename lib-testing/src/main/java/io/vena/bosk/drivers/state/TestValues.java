package io.vena.bosk.drivers.state;

import io.vena.bosk.ListValue;
import io.vena.bosk.MapValue;
import io.vena.bosk.StateTreeNode;
import lombok.Value;
import lombok.With;
import lombok.experimental.Accessors;
import lombok.experimental.FieldNameConstants;

@Value
@Accessors(fluent = true)
@With
@FieldNameConstants
public class TestValues implements StateTreeNode {
	String string;
	ListValue<String> list;
	MapValue<String> map;

	public static TestValues blank() {
		return new TestValues("", ListValue.empty(), MapValue.empty());
	}
}
