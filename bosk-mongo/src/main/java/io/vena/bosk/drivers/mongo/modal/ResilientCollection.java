package io.vena.bosk.drivers.mongo.modal;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.bson.Document;
import org.bson.conversions.Bson;

public class ResilientCollection implements BoskCollection {
	private volatile State currentState;

	@RequiredArgsConstructor
	private static class State {
		final MongoCollection<Document> collection;
	}

	public void reconnect(@NonNull MongoCollection<Document> collection) {
		currentState = new State(collection);
	}

	@Override
	public FindIterable<Document> find(Bson filter) {
		State state = currentState;
		if (state == null) {
			throw new DisconnectedException();
		} else {
			return state.collection.find(filter);
		}
	}

	@Override
	public UpdateResult updateOne(Bson filter, Bson update, UpdateOptions updateOptions) {
		State state = currentState;
		if (state == null) {
			throw new DisconnectedException();
		} else {
			return state.collection.updateOne(filter, update, updateOptions);
		}
	}

	@Override
	public UpdateResult replaceOne(Bson filter, Document replacement) {
		State state = currentState;
		if (state == null) {
			throw new DisconnectedException();
		} else {
			return state.collection.replaceOne(filter, replacement);
		}
	}
}
