package io.vena.bosk.drivers.mongo.v2;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.Entity;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.drivers.mongo.MongoDriver;
import io.vena.bosk.exceptions.InvalidTypeException;
import io.vena.bosk.exceptions.NotYetImplementedException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainDriver<R extends Entity> implements MongoDriver<R> {
	private final Lock lock = new ReentrantLock();
	private final BoskDriver<R> downstream;
	private final Reference<R> rootRef;
	private final ChangeEventReceiver receiver;

	private volatile FormatDriver<R> formatDriver;

	MainDriver(MongoClient client, BoskDriver<R> downstream, Reference<R> rootRef) {
		this.downstream = downstream;
		this.rootRef = rootRef;

		this.receiver = new ChangeEventReceiver(
			client.getDatabase("foo").getCollection("bar"));
	}

	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		R result = initializeReplication();
		receiver.start();
		if (result == null) {
			return downstream.initialRoot(rootType);
		} else {
			return result;
		}
	}

	private void recoverFrom(Exception e) {
		LOGGER.error("Unexpected exception; reinitializing", e);
		R result = initializeReplication();
		if (result != null) {
			downstream.submitReplacement(rootRef, result);
		}
		receiver.start();
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		formatDriver.submitReplacement(target, newValue);
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		formatDriver.submitConditionalReplacement(target, newValue, precondition, requiredValue);
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		formatDriver.submitInitialization(target, newValue);
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		formatDriver.submitDeletion(target);
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		formatDriver.submitConditionalDeletion(target, precondition, requiredValue);
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		formatDriver.flush();
	}

	@Override
	public void refurbish() {
		throw new NotYetImplementedException();
	}

	@Override
	public void close() {
		receiver.close();
		formatDriver.close();
	}

	/**
	 * Reinitializes {@link #receiver}, detects the database format, instantiates
	 * the appropriate {@link FormatDriver}, and uses it to load the initial bosk state.
	 * <p>
	 * Caller is responsible for calling {@link #receiver}{@link ChangeEventReceiver#start() .start()}
	 * to kick off event processing. We don't do it here because some callers need to do other things
	 * after initialization but before any events arrive.
	 *
	 * @return The new root object to use, if any
	 */
	private R initializeReplication() {
		try {
			lock.lock();
			formatDriver = new DisconnectedDriver<>(); // In case initialization fails
			if (receiver.initialize(new Listener())) {
				DummyFormatDriver<R> newDriver = new DummyFormatDriver<>(); // TODO: Determine the right one
				StateResult<R> result = newDriver.loadAllState();
				newDriver.onRevisionToSkip(result.revision);
				formatDriver = newDriver;
				return result.state;
			} else {
				LOGGER.warn("Unable to fetch resume token");
				return null;
			}
		} catch (ReceiverInitializationException e) {
			LOGGER.warn("Failed to initialize replication", e);
			return null;
		} finally {
			assert formatDriver != null;
			lock.unlock();
		}
	}

	private final class Listener implements ChangeEventListener {
		/**
		 * Raise your hand if you want to think about the case where a listener keeps on processing
		 * events after an exception. Nobody? Ok, that's what I thought.
		 */
		volatile boolean isListening = true; // (volatile is probably overkill because all calls are on the same thread anyway)

		@Override
		public void onEvent(ChangeStreamDocument<Document> event) {
			if (isListening) {
				formatDriver.onEvent(event);
			}
		}

		@Override
		public void onException(Exception e) {
			isListening = false;
			recoverFrom(e);
		}
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(MainDriver.class);
}
