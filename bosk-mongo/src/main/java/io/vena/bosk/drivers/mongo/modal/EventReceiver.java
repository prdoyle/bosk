package io.vena.bosk.drivers.mongo.modal;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;

public interface EventReceiver {
	/**
	 * {@link com.mongodb.client.model.changestream.OperationType#INSERT INSERT} or
	 * {@link com.mongodb.client.model.changestream.OperationType#UPDATE UPDATE} events.
	 * Changes an entire document.  {@link ChangeStreamDocument#getFullDocument} is reliable.
	 */
	void onUpsert(ChangeStreamDocument<Document> event);

	/**
	 * {@link com.mongodb.client.model.changestream.OperationType#UPDATE UPDATE} events.
	 */
	void onUpdate(ChangeStreamDocument<Document> event);

	void onUnrecognizedEvent(ChangeStreamDocument<Document> event);

	void onException(Exception e);
}
