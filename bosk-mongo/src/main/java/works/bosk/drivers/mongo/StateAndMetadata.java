package works.bosk.drivers.mongo;

import org.bson.BsonInt64;
import works.bosk.MapValue;
import works.bosk.StateTreeNode;

record StateAndMetadata<R extends StateTreeNode>(
	R state,
	BsonInt64 revision,
	MapValue<String> diagnosticAttributes
) { }
