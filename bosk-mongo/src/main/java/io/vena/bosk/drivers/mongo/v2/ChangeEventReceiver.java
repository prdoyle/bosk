package io.vena.bosk.drivers.mongo.v2;

import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.vena.bosk.exceptions.NotYetImplementedException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.RequiredArgsConstructor;
import org.bson.BsonDocument;
import org.bson.Document;

@RequiredArgsConstructor
public class ChangeEventReceiver {
	private final MongoCollection<Document> collection;
	private final ExecutorService ex = Executors.newFixedThreadPool(1);

	private volatile State current;
	private volatile BsonDocument lastProcessedResumeToken;

	@RequiredArgsConstructor
	private static final class State {
		final MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor;
		final ChangeEventListener receiver;
	}

	void reinitialize(ChangeEventListener receiver) throws ReinitializationException {
		try {
			State newState;
			if (lastProcessedResumeToken == null) {
				throw new NotYetImplementedException("No resume token - coordinate with state reload");
			}
			newState = new State(
				collection.watch()
					.resumeAfter(lastProcessedResumeToken)
					.cursor(),
				receiver
			);
			ChangeStreamDocument<Document> event = newState.cursor.tryNext();
			if (event == null) {
				throw new NotYetImplementedException("Process first event, but do it on the event-handling thread");
			}
			this.current = newState;
		} catch (RuntimeException e) {
			throw new ReinitializationException(e);
		}
	}

	void eventProcessingLoop(State state, ChangeStreamDocument<Document> initialEvent) {
		if (initialEvent != null) {
			processEvent(initialEvent);
		}
	}

	private void processEvent(ChangeStreamDocument<Document> event) {
	}
}
