package io.vena.bosk.drivers.mongo.v3;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.vena.bosk.drivers.mongo.v2.UnprocessableEventException;
import org.bson.Document;

interface ChangeEventListener {
	void onEvent(ChangeStreamDocument<Document> event) throws UnprocessableEventException;
	void onException(Exception e);
}
