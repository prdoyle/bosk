package io.vena.bosk.drivers.mongo.v3;

import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.vena.bosk.drivers.mongo.MongoDriverSettings;
import io.vena.bosk.drivers.mongo.v3.MappedDiagnosticContext.MDCScope;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.vena.bosk.drivers.mongo.v3.MappedDiagnosticContext.setupMDC;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class ChangeReceiver implements Closeable {
	private final String boskName;
	private final ChangeListener listener;
	private final MongoDriverSettings settings;
	private final MongoCollection<Document> collection;
	private final ScheduledExecutorService ex = Executors.newScheduledThreadPool(1);
	private volatile boolean isClosed = false;

	ChangeReceiver(String boskName, ChangeListener listener, MongoDriverSettings settings, MongoCollection<Document> collection) {
		this.boskName = boskName;
		this.listener = listener;
		this.settings = settings;
		this.collection = collection;
		ex.scheduleWithFixedDelay(
			this::connectionLoop,
			0,
			settings.recoveryPollingMS(),
			MILLISECONDS
		);
	}

	@Override
	public void close() throws IOException {
		isClosed = true;
		ex.shutdown();
	}

	/**
	 * This method has a loop to do immediate reconnections and skip the
	 * {@link MongoDriverSettings#recoveryPollingMS() recoveryPollingMS} delay,
	 * but besides that, exiting this method has the same effect as continuing
	 * around the loop.
	 */
	private void connectionLoop() {
		String oldThreadName = currentThread().getName();
		currentThread().setName(getClass().getSimpleName() + " [" + boskName + "]");
		try (MDCScope __ = setupMDC(boskName)) {
			try {
				while (!isClosed) {
					try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = openCursor()) {
						listener.onConnect();

						// Note that eventLoop does not throw RuntimeException; therefore,
						// any RuntimeException must have occurred before this point.
						eventLoop(cursor);
					} catch (UnprocessableEventException e) {
						LOGGER.warn("Unprocessable event; reconnecting", e);
						listener.onDisconnect(e);
					} catch (UnexpectedEventProcessingException|IOException|InterruptedException|UninitializedCollectionException e) {
						LOGGER.warn("Unexpected exception during event processing; reconnecting", e);
						listener.onDisconnect(e);
					} catch (UnrecognizedFormatException e) {
						LOGGER.warn("Unrecognized datbase format; will wait and retry", e);
						listener.onDisconnect(e);
						return;
					} catch (RuntimeException e) {
						LOGGER.warn("Change stream connection failed; will wait and retry", e);
						listener.onDisconnect(e);
						return;
					}
				}
			} finally {
				currentThread().setName(oldThreadName);
			}
		}
	}

	private MongoChangeStreamCursor<ChangeStreamDocument<Document>> openCursor() {
		return collection
			.watch()
			.maxAwaitTime(settings.recoveryPollingMS(), MILLISECONDS)
			.cursor();
	}

	/**
	 * Should not throw RuntimeException, or else {@link #connectionLoop()} is likely to overreact.
	 */
	private void eventLoop(MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor) throws UnprocessableEventException, UnexpectedEventProcessingException {
		try {
			while (!isClosed) {
				processEvent(cursor.next());
			}
		} catch (RuntimeException e) {
			throw new UnexpectedEventProcessingException(e);
		}
	}

	private void processEvent(ChangeStreamDocument<Document> event) throws UnprocessableEventException {
		listener.onEvent(event);
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(ChangeReceiver.class);
}
