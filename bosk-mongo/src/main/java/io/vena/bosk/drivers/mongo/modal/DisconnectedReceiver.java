package io.vena.bosk.drivers.mongo.modal;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;

/**
 * A receiver that ignores all events
 */
public class DisconnectedReceiver implements EventReceiver {
	@Override public void onUpsert(ChangeStreamDocument<Document> event) { }
	@Override public void onUpdate(ChangeStreamDocument<Document> event) { }
	@Override public void onUnrecognizedEvent(ChangeStreamDocument<Document> event) { }
	@Override public void onException(Exception e) { }
}
