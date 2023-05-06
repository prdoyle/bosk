package io.vena.bosk.drivers.mongo;

import com.mongodb.MongoClientSettings;
import io.vena.bosk.Bosk;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.Entity;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.drivers.mongo.modal.DisconnectedModeDriver;
import io.vena.bosk.drivers.mongo.modal.FutureMongoDriver;
import io.vena.bosk.drivers.mongo.modal.ModalDriverFacade;
import io.vena.bosk.drivers.mongo.modal.ReconnectingModeDriver;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.concurrent.CyclicBarrier;

import static io.vena.bosk.drivers.mongo.SingleDocumentMongoDriver.validateMongoClientSettings;

public class MainMongoDriver<R extends Entity> implements MongoDriver<R> {
	private final ModalDriverFacade<R> facade;

	private final ReconnectingModeDriver<R> reconnectingModeDriver;
	private final DisconnectedModeDriver<R> disconnectedModeDriver;
	private final CyclicBarrier reconnectFinished = new CyclicBarrier()

	public MainMongoDriver(Bosk<R> bosk, MongoClientSettings clientSettings, MongoDriverSettings driverSettings, BsonPlugin bsonPlugin, BoskDriver<R> downstream) {
		validateMongoClientSettings(clientSettings);
		FutureMongoDriver<R> future;
		reconnectingModeDriver = new ReconnectingModeDriver<>(future);
		disconnectedModeDriver = new DisconnectedModeDriver<>(downstream);
		facade = new ModalDriverFacade<>(reconnectingModeDriver);
	}

	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		return null;
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {

	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {

	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {

	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {

	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {

	}

	@Override
	public void flush() throws IOException, InterruptedException {

	}

	@Override
	public void refurbish() {

	}

	@Override
	public void close() {

	}
}
