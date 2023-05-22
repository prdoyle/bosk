package io.vena.bosk.drivers.mongo.v2;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.vena.bosk.Entity;
import io.vena.bosk.drivers.mongo.MongoDriver;
import org.bson.BsonInt64;
import org.bson.Document;

/**
 * Additional {@link MongoDriver} functionality that the format-specific drivers must implement.
 */
public interface FormatDriver<R extends Entity> extends MongoDriver<R> {
	StateResult<R> loadAllState();
	void onEvent(ChangeStreamDocument<Document> event);
	void onRevisionToSkip(BsonInt64 revision);
}
