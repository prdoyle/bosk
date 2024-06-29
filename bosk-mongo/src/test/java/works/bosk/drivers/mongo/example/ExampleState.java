package works.bosk.drivers.mongo.example;

import works.bosk.StateTreeNode;
import lombok.Value;

@Value
public class ExampleState implements StateTreeNode {
	// Add fields here as you need them
	String name;
}
