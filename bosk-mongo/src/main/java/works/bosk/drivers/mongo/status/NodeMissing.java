package works.bosk.drivers.mongo.status;

import static works.bosk.drivers.mongo.status.Difference.prefixed;

public record NodeMissing(
	String bsonPath
) implements SomeDifference {
	@Override
	public NodeMissing withPrefix(String prefix) {
		return new NodeMissing(prefixed(prefix, bsonPath));
	}
}
