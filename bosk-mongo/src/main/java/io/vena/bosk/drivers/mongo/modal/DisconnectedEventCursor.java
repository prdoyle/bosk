package io.vena.bosk.drivers.mongo.modal;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;

public class DisconnectedEventCursor implements BoskEventCursor{
	@Override
	public ChangeStreamDocument<Document> next() {
		throw new UnsupportedOperationException("Cannot fetch events");
	}
}
