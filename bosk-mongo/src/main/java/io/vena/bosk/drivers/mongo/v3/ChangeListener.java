package io.vena.bosk.drivers.mongo.v3;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import java.io.IOException;
import org.bson.Document;

interface ChangeListener {
	void onConnect() throws UnrecognizedFormatException, UninitializedCollectionException, InterruptedException, IOException;
	void onEvent(ChangeStreamDocument<Document> event) throws UnprocessableEventException;
	void onDisconnect(Exception e);
}
