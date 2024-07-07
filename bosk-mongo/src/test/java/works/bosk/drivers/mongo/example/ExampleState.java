package works.bosk.drivers.mongo.example;

import lombok.Value;
import works.bosk.StateTreeNode;

@Value
public class ExampleState implements StateTreeNode {
	// Add fields here as you need them
	String name;
}
