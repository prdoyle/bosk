package io.vena.bosk.drivers.mongo.v2;

import lombok.RequiredArgsConstructor;
import org.bson.BsonDocument;
import org.bson.BsonInt64;

@RequiredArgsConstructor
class StateResult {
	final BsonDocument state;
	final BsonInt64 revision;
}
