package io.vena.bosk.drivers.mongo.modal;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;

public interface EventCursor {
	ChangeStreamDocument<Document> next();
}
