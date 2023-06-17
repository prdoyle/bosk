package io.vena.bosk.drivers.mongo.v3;

import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.vena.bosk.drivers.mongo.MongoDriverSettings;
import io.vena.bosk.drivers.mongo.v2.ChangeEventListener;
import io.vena.bosk.drivers.mongo.v2.ReceiverInitializationException;
import io.vena.bosk.exceptions.TunneledCheckedException;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

class ChangeEventAgent implements Closeable {
	private final ScheduledExecutorService ex = Executors.newScheduledThreadPool(1);
	private final ResettableDriver<?> resettableDriver;

	ChangeEventAgent(ResettableDriver<?> resettableDriver, MongoDriverSettings settings) {
		this.resettableDriver = resettableDriver;
		ex.scheduleWithFixedDelay(
			this::eventProcessingTask,
			0,
			settings.recoveryPollingMS(),
			MILLISECONDS);
	}

	private void eventProcessingTask() {
		final MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor;
		final ChangeEventListener listener;
		ChangeStreamDocument<Document> initialEvent; // Could be final, but we want to let the GC collect it
		boolean isClosed;

		try {
			while (true) {
				connect();
				try {
					while (true) {
						processEvent(cursor.next());
					}
				} catch (CertainSpecificExceptions) {
					// log, disconnect, and reconnect immediately
				} finally {
					disconnect();
				}
			}
		} finally {
			discardResumeToken();
		}
	}

	private void connect() {
	}

	private void disconnect() {
	}

	private void processEvent(ChangeStreamDocument<Document> next) {
	}

	private void discardResumeToken() {

	}

	private void initializeReplication() {
	}

	@Override
	public void close() {
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventAgent.class);
}
