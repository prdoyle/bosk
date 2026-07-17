package works.bosk.drivers.mongo.internal;

import org.bson.BsonInt64;
import org.jspecify.annotations.Nullable;
import works.bosk.StateTreeNode;
import works.bosk.util.PerTenantValue;

record AllState<R extends StateTreeNode>(
	PerTenantValue<StateAndMetadata<R>> contents,
	@Nullable BsonInt64 contentsRevision
) {
}
