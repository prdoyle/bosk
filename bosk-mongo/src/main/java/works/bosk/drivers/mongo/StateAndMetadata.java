package works.bosk.drivers.mongo;

import works.bosk.MapValue;
import works.bosk.StateTreeNode;
import org.bson.BsonInt64;

record StateAndMetadata<R extends StateTreeNode>(
	R state,
	BsonInt64 revision,
	MapValue<String> diagnosticAttributes
) { }