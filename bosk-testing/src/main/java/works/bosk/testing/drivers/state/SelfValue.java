package works.bosk.testing.drivers.state;

import works.bosk.Reference;
import works.bosk.StateTreeNode;
import works.bosk.annotations.Self;

public record SelfValue(
	@Self Reference<SelfValue> self,
	String string
) implements StateTreeNode {}
