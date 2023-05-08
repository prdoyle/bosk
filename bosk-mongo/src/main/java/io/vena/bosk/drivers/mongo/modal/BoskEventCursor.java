package io.vena.bosk.drivers.mongo.modal;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;

/**
 * An abstraction of {@link com.mongodb.client.MongoCursor} suitable for receiving
 * change events from a {@link BoskCollection}.
 */
public interface BoskEventCursor {
	ChangeStreamDocument<Document> next();
}
