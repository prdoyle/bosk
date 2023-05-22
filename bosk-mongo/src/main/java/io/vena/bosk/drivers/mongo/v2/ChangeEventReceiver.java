package io.vena.bosk.drivers.mongo.v2;

import com.mongodb.MongoInterruptedException;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.vena.bosk.exceptions.NotYetImplementedException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.RequiredArgsConstructor;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.SECONDS;

@RequiredArgsConstructor
class ChangeEventReceiver {
	private final MongoCollection<Document> collection;
	private final ExecutorService ex = Executors.newFixedThreadPool(1);

	private final Lock lock = new ReentrantLock();
	private volatile State current;
	private volatile BsonDocument lastProcessedResumeToken;
	private volatile Future<Void> eventProcessingTask;

	@RequiredArgsConstructor
	private static final class State {
		final MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor;
		final ChangeStreamDocument<Document> initialEvent;
		final ChangeEventListener listener;
	}

	/**
	 * Sets up an event processing loop so that it will start feeding events to
	 * <code>newListener</code> when {@link #start()} is called.
	 * Shuts down the existing event processing loop, if any.
	 */
	public void initialize(ChangeEventListener newListener) throws ReceiverInitializationException {
		try {
			lock.lock();
			stop();
			setupNewState(newListener);
		} catch (RuntimeException | InterruptedException | TimeoutException e) {
			throw new ReceiverInitializationException(e);
		} finally {
			lock.unlock();
		}
	}

	public void start() {
		try {
			lock.lock();
			if (current == null) {
				throw new IllegalStateException("Receiver is not initialized");
			}
			ex.submit(()->eventProcessingLoop(current));
		} finally {
			lock.unlock();
		}
	}

	public void stop() throws InterruptedException, TimeoutException {
		try {
			lock.lock();
			Future<Void> task = this.eventProcessingTask;
			if (task != null) {
				task.cancel(true);
				task.get(10, SECONDS); // TODO: Config
				this.eventProcessingTask = null;
			}
		} catch (ExecutionException e) {
			throw new NotYetImplementedException("Event processing loop isn't supposed to throw!", e);
		} finally {
			lock.unlock();
		}
	}

	private void setupNewState(ChangeEventListener newListener) throws ReceiverInitializationException {
		this.current = null; // In case any exceptions happen during this method

		MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor;
		ChangeStreamDocument<Document> initialEvent;
		if (lastProcessedResumeToken == null) {
			cursor = collection.watch().cursor();
			initialEvent = cursor.tryNext();
			if (cursor.getResumeToken() == null) {
				throw new ReceiverInitializationException("Unable to get resume token");
			}
			if (initialEvent == null) {
				// In this case, tryNext() has caused the cursor to point to
				// a token in the past, so we can reliably use that.
				lastProcessedResumeToken = cursor.getResumeToken();
			}
		} else {
			cursor = collection.watch().resumeAfter(lastProcessedResumeToken).cursor();
			initialEvent = null;
		}
		if (lastProcessedResumeToken == null) {
			throw new NotYetImplementedException("No resume token - coordinate with state reload");
		}
		current = new State(cursor, initialEvent, newListener);
	}

	/**
	 * This method has no uncaught exceptions. They're all reported to {@link ChangeEventListener#onException}.
	 */
	private void eventProcessingLoop(State state) {
		try {
			if (state.initialEvent != null) {
				processEvent(state, state.initialEvent);
			}
			while (true) {
				processEvent(state, state.cursor.next());
			}
		} catch (MongoInterruptedException e) {
			LOGGER.debug("Event loop interrupted", e);
			state.listener.onException(e);
		} catch (RuntimeException e) {
			LOGGER.info("Unexpected exception; event loop aborted", e);
			state.listener.onException(e);
		}
	}

	private void processEvent(State state, ChangeStreamDocument<Document> event) {
		state.listener.onEvent(event);
		lastProcessedResumeToken = event.getResumeToken();
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventReceiver.class);
}
