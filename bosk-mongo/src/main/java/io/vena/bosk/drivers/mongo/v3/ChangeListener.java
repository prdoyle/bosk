package io.vena.bosk.drivers.mongo.v3;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.bson.Document;

interface ChangeListener {
	void onConnect() throws
		UnrecognizedFormatException,
		UninitializedCollectionException,
		InterruptedException,
		IOException,
		InitialRootException,
		TimeoutException;

	void onEvent(ChangeStreamDocument<Document> event) throws UnprocessableEventException;

	void onDisconnect(Exception e);
}
