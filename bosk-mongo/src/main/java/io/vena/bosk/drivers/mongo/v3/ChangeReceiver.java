package io.vena.bosk.drivers.mongo.v3;

import com.mongodb.MongoInterruptedException;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.vena.bosk.drivers.mongo.MongoDriverSettings;
import io.vena.bosk.drivers.mongo.v3.MappedDiagnosticContext.MDCScope;
import java.io.Closeable;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.vena.bosk.drivers.mongo.v3.MappedDiagnosticContext.setupMDC;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Houses a background thread that repeatedly initializes, processes, and closes a change stream cursor.
 * Ideally, the opening and closing happen just once, but they're done in a loop for fault tolerance,
 * so that the driver can reinitialize if certain unusual conditions arise.
 */
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
	public void close() {
		isClosed = true;
		ex.shutdownNow();
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
			LOGGER.debug("Starting connectionLoop task");
			try {
				while (!isClosed) {
					// Design notes:
					//
					// For the following try-catch clause, a `continue` causes us to attempt an immediate reconnection,
					// while a `return` causes us to wait for the "recovery polling" interval to elapse first.
					// When in doubt, `return` is a bit safer because it's unlikely to cause a spin-loop of rapid reconnections.
					//
					// Log `warn` and `error` levels are likely to be logged by applications in production,
					// and so they will be visible to whatever team is operating the application that uses Bosk.
					// They should be written with a reader in mind who is not a Bosk expert, perhaps not even
					// knowing what bosk is, and should contain enough information to guide their troubleshooting efforts.
					//
					// Logs at `info` levels can target knowledgeable Bosk users, and should aim to explain what
					// the library is doing in a way that helps them learn to use it more effectively.
					//
					// Logs at the `debug` level target Bosk developers. They can use some Bosk jargon, though
					// they should also be helping new Bosk developers climb the learning curve. They should
					// allow developers to tell what code paths executed.
					//
					// Logs at the `trace` level target expert Bosk developers troubleshooting very tricky bugs,
					// and can include information that would be too voluminous to emit under most circumstances.
					// Examples include stack traces for routine situations, or dumps of entire data structures,
					// neither of which should be done at the `debug` level. It can also include high-frequency messages
					// emitted many times for a single user action (again, not recommended at the `debug` level),
					// though this must be done cautiously, since even disabled log statements still have nonzero overhead.
					//
					LOGGER.debug("Opening cursor");
					try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = openCursor()) {
						try {
							listener.onConnectionSucceeded();

							// Note that eventLoop does not throw RuntimeException; therefore,
							// any RuntimeException must have occurred before this point.
							// TODO: Two try blocks?
							eventLoop(cursor);
						} catch (UnprocessableEventException|UnexpectedEventProcessingException e) {
							LOGGER.warn("Unable to process change event; reconnecting: {}", e.toString(), e);
							listener.onDisconnect(e);
							// Reconnection will skip this event, so it's safe to try it right away
							continue;
						} catch (InterruptedException e) {
							LOGGER.warn("Interrupted while processing change events; reconnecting", e);
							listener.onDisconnect(e);
							continue;
						} catch (IOException e) {
							LOGGER.warn("Unexpected exception during change event processing; will wait and retry", e);
							listener.onDisconnect(e);
							return;
						} catch (UnrecognizedFormatException e) {
							LOGGER.warn("Unrecognized database format; will wait and retry", e);
							listener.onDisconnect(e);
							return;
						} catch (UninitializedCollectionException e) {
							LOGGER.warn("Database collection is not initialized; will wait and retry", e);
							listener.onDisconnect(e);
							return;
						} catch (InitialRootException e) {
							LOGGER.warn("Unable to initialize bosk state; will wait and retry", e);
							listener.onDisconnect(e);
							return;
						} catch (TimeoutException e) {
							LOGGER.warn("Timed out waiting for bosk state to initialize; will wait and retry", e);
							listener.onDisconnect(e);
							return;
						} catch (RuntimeException e) {
							LOGGER.warn("Unexpected exception from onConnectionSucceeded; will wait and retry", e);
							listener.onDisconnect(e);
							return;
						}
					} catch (RuntimeException e) {
						LOGGER.warn("Unable to connect to database; will wait and retry", e);
						try {
							listener.onConnectionFailed(e);
						} catch (InterruptedException | InitialRootException | TimeoutException e2) {
							LOGGER.error("Error while running connection failure handler; will wait and reconnect", e2);
						}
						return;
					}
					LOGGER.trace("Change event processing returned normally");
				}
			} finally {
				LOGGER.debug("Ending connectionLoop task; isClosed={}", isClosed);
				currentThread().setName(oldThreadName);
			}
		} catch (RuntimeException e) {
			LOGGER.warn("connectionLoop task ended with unexpected {}; discarding", e.getClass().getSimpleName(), e);
		}
	}

	private MongoChangeStreamCursor<ChangeStreamDocument<Document>> openCursor() {
		try {
			return collection
				.watch()
				.maxAwaitTime(settings.recoveryPollingMS(), MILLISECONDS)
				.cursor();
		} finally {
			LOGGER.debug("Cursor is open");
		}
	}

	/**
	 * Should not throw RuntimeException, or else {@link #connectionLoop()} is likely to overreact.
	 */
	private void eventLoop(MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor) throws UnprocessableEventException, UnexpectedEventProcessingException {
		if (isClosed) {
			LOGGER.debug("Receiver is closed");
			return;
		}
		try {
			LOGGER.debug("Starting event loop");
			while (!isClosed) {
				ChangeStreamDocument<Document> event;
				try {
					event = cursor.next();
				} catch (NoSuchElementException e) {
					LOGGER.debug("Cursor is finished");
					break;
				} catch (MongoInterruptedException e) {
					LOGGER.debug("Interrupted while waiting for change event: {}", e.toString());
					break;
				}
				processEvent(event);
			}
		} catch (RuntimeException e) {
			LOGGER.debug("Unexpected exception while processing events", e);
			throw new UnexpectedEventProcessingException(e);
		} finally {
			LOGGER.debug("Exited event loop");
		}
	}

	private void processEvent(ChangeStreamDocument<Document> event) throws UnprocessableEventException {
		listener.onEvent(event);
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(ChangeReceiver.class);
}
