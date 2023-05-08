package io.vena.bosk.drivers.mongo.modal;

import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A mutable container for a pair of ({@link BoskEventCursor}, {@link EventReceiver}).
 * Feeds the events from the cursor to the receiver.
 * On exceptions, informs the receiver, which typically uses {@link #reconnect} to
 * substitute a new cursor and receiver.
 */
public class ResilientListener {
	/**
	 * null means this listener is closed, and is permanently unusable.
	 */
	private volatile State currentState;

	private final ExecutorService ex = Executors.newFixedThreadPool(1);

	@RequiredArgsConstructor
	private static final class State {
		final BoskEventCursor eventCursor;
		final EventReceiver receiver;
	}

	public ResilientListener(@NonNull BoskEventCursor eventCursor, @NonNull EventReceiver receiver) {
		reconnect(eventCursor, receiver);
		ex.submit(this::eventLoop);
	}

	public void close() {
		currentState = null;
		ex.shutdownNow();
	}

	public void reconnect(@NonNull BoskEventCursor eventCursor, @NonNull EventReceiver receiver) {
		currentState = new State(eventCursor, receiver);
	}

	private void eventLoop() {
		State state;
		while ((state = currentState) != null) {
			try {
				try {
					ChangeStreamDocument<Document> event = state.eventCursor.next();
					switch (event.getOperationType()) {
						case INSERT:
						case REPLACE:
							state.receiver.onUpsert(event);
							break;
						case UPDATE:
							state.receiver.onUpdate(event);
							break;
						default:
							LOGGER.info("Unrecognized event: {}", event.getOperationType());
							state.receiver.onUnrecognizedEvent(event);
							break;
					}
				} catch (MongoInterruptedException e) {
					LOGGER.info("Event loop interrupted", e);
					state.receiver.onException(e);
				} catch (MongoException e) {
					LOGGER.info("Mongo exception", e);
					state.receiver.onException(e);
				} catch (RuntimeException e) {
					LOGGER.warn("Unexpected exception", e);
					state.receiver.onException(e);
				}
			} catch (RuntimeException e) {
				LOGGER.error("Receiver.onException crashed; re-trying", e);
				try {
					state.receiver.onException(e);
				} catch (RuntimeException e2) {
					LOGGER.error("Receiver.onException crashed again; ignoring", e2);
				}
			}
		}
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(ResilientListener.class);
}
