package io.vena.bosk.drivers.mongo;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.vena.bosk.Bosk;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.Entity;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.drivers.mongo.modal.ConnectedModeDriver;
import io.vena.bosk.drivers.mongo.modal.DisconnectedEventCursor;
import io.vena.bosk.drivers.mongo.modal.DisconnectedException;
import io.vena.bosk.drivers.mongo.modal.DisconnectedModeDriver;
import io.vena.bosk.drivers.mongo.modal.DisconnectedReceiver;
import io.vena.bosk.drivers.mongo.modal.ModalDriverFacade;
import io.vena.bosk.drivers.mongo.modal.ReconnectingModeDriver;
import io.vena.bosk.drivers.mongo.modal.ResilientCollection;
import io.vena.bosk.drivers.mongo.modal.ResilientListener;
import io.vena.bosk.drivers.mongo.modal.SingleDocFormatDriver;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.concurrent.FutureTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.vena.bosk.drivers.mongo.SingleDocumentMongoDriver.COLLECTION_NAME;
import static io.vena.bosk.drivers.mongo.SingleDocumentMongoDriver.validateMongoClientSettings;

/**
 * This is the top-level driver implementation object.
 * It is focused on fault tolerance, orchestrating mode changes and reconnect operations.
 * The actual database and bosk interactions are left to other objects.
 */
public class MainMongoDriver<R extends Entity> implements MongoDriver<R> {
	private final Lock modeChangeLock = new ReentrantLock();
	private final BoskDriver<R> downstream;
	private final ModalDriverFacade<R> facade;
	private final MongoClient mongoClient;
	private final ResilientCollection collection;
	private final ResilientListener listener;

	private final DisconnectedModeDriver<R> disconnectedModeDriver;

	private final MongoClientSettings clientSettings;
	private final MongoDriverSettings driverSettings;
	private volatile boolean isClosed;

	public MainMongoDriver(Bosk<R> bosk, MongoClientSettings clientSettings, MongoDriverSettings driverSettings, BsonPlugin bsonPlugin, BoskDriver<R> downstream) {
		validateMongoClientSettings(clientSettings);
		this.mongoClient = MongoClients.create(clientSettings);
		this.clientSettings = clientSettings;
		this.driverSettings = driverSettings;
		this.downstream = downstream;
		this.disconnectedModeDriver = new DisconnectedModeDriver<>(downstream);
		this.facade = new ModalDriverFacade<>(disconnectedModeDriver);
		this.collection = new ResilientCollection();
		this.listener = new ResilientListener(new DisconnectedEventCursor(), new DisconnectedReceiver());
		reconnect();
	}

	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		// TODO: This is only appropriate as long as we are initially reconnecting,
		// or if we're initializing the database state.
		return downstream.initialRoot(rootType);
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		runAndHandleExceptions(()->
			facade.submitReplacement(target, newValue));
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		runAndHandleExceptions(()->
			facade.submitConditionalReplacement(target, newValue, precondition, requiredValue));
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		runAndHandleExceptions(()->
			facade.submitInitialization(target, newValue));
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		runAndHandleExceptions(()->
			facade.submitDeletion(target));
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		runAndHandleExceptions(()->
			facade.submitConditionalDeletion(target, precondition, requiredValue));
	}

	@Override
	public void refurbish() {
		runAndHandleExceptions(facade::refurbish);
	}

	@Override
	public void close() {
		isClosed = true;
		disconnect();
		facade.close();
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		while (true) {
			if (isClosed) {
				throw new IllegalStateException("Driver is closed");
			}
			try {
				facade.flush();
				return;
			} catch (DisconnectedException e) {
				throw e;
			} catch (RuntimeException e) {
				handleException(e);
			}
		}
	}

	private void runAndHandleExceptions(Runnable action) {
		while (true) {
			if (isClosed) {
				throw new IllegalStateException("Driver is closed");
			}
			try {
				action.run();
				return;
			} catch (DisconnectedException e) {
				throw e;
			} catch (RuntimeException e) {
				handleException(e);
			}
		}
	}

	/**
	 * Initiates a reconnect attempt, and swaps the facade's implementation for
	 * a {@link ReconnectingModeDriver} that will cause all driver calls to wait
	 * for that attempt to succeed.
	 */
	private void reconnect() {
		modeChangeLock.lock();
		try {
			MongoDriver<R> currentImplementation = facade.currentImplementation();
			if (currentImplementation instanceof ConnectedModeDriver) {
				FutureTask<MongoDriver<R>> reconnectTask = new FutureTask<>(this::reconnectTask);
				MongoDriver<R> newImplementation = new ReconnectingModeDriver<>(reconnectTask);
				if (facade.changeImplementation(currentImplementation, newImplementation)) {
					LOGGER.info("Initiating reconnect");
					reconnectTask.run();
				} else {
					LOGGER.info("Driver changed concurrently; abandoned redundant reconnect");
				}
			} else {
				LOGGER.info("Skipped reconnect. Driver is already a {}", currentImplementation.getClass().getSimpleName());
			}
		} finally {
			modeChangeLock.unlock();
		}
	}

	/**
	 * @return A driver that any pending driver calls to {@link ReconnectingModeDriver} will forward to.
	 */
	private MongoDriver<R> reconnectTask() {
		// Note:  The driver returned from this one will be called from ReconnectingModeDriver,
		// so don't return a ReconnectingModeDriver here without being very careful
		// to avoid infinite loop/recursion.
		try {
			collection.reconnect(mongoClient
				.getDatabase(driverSettings.database())
				.getCollection(COLLECTION_NAME));
			ConnectedModeDriver<R> newDriver = new ConnectedModeDriver<>(
				new SingleDocFormatDriver<>(collection));
			listener.reconnect();
			facade.changeImplementation()
			return newDriver;
		} catch (RuntimeException e) {
			LOGGER.error("Unable to reconnect", e);
			disconnect();
			return disconnectedModeDriver;
		}
	}

	private void disconnect() {
		facade.changeImplementation(facade.currentImplementation(), disconnectedModeDriver);
	}

	private void handleException(RuntimeException e) {
		// TODO: We probably don't want to reconnect on every single exception?
		LOGGER.warn("MongoDriver operation encountered exception; will reconnect", e);
		reconnect();
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(MainMongoDriver.class);

}
