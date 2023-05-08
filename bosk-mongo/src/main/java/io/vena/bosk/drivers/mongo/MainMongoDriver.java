package io.vena.bosk.drivers.mongo;

import com.mongodb.MongoClientSettings;
import io.vena.bosk.Bosk;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.Entity;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.drivers.mongo.modal.DisconnectedEventCursor;
import io.vena.bosk.drivers.mongo.modal.DisconnectedModeDriver;
import io.vena.bosk.drivers.mongo.modal.DisconnectedReceiver;
import io.vena.bosk.drivers.mongo.modal.ModalDriverFacade;
import io.vena.bosk.drivers.mongo.modal.ResilientListener;
import io.vena.bosk.exceptions.InvalidTypeException;
import io.vena.bosk.exceptions.NotYetImplementedException;
import java.io.IOException;
import java.lang.reflect.Type;

import static io.vena.bosk.drivers.mongo.SingleDocumentMongoDriver.validateMongoClientSettings;

public class MainMongoDriver<R extends Entity> implements MongoDriver<R> {
	private final BoskDriver<R> downstream;
	private final ModalDriverFacade<R> facade;
	private final ResilientListener listener;

	private final DisconnectedModeDriver<R> disconnectedModeDriver;

	private final DisconnectedEventCursor disconnectedEventCursor = new DisconnectedEventCursor();
	private final DisconnectedReceiver disconnectedReceiver = new DisconnectedReceiver();

	public MainMongoDriver(Bosk<R> bosk, MongoClientSettings clientSettings, MongoDriverSettings driverSettings, BsonPlugin bsonPlugin, BoskDriver<R> downstream) {
		validateMongoClientSettings(clientSettings);
		this.downstream = downstream;
		this.disconnectedModeDriver = new DisconnectedModeDriver<>(downstream);
		this.facade = new ModalDriverFacade<>(disconnectedModeDriver);
		this.listener = new ResilientListener(disconnectedEventCursor, disconnectedReceiver);
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
		try {
			facade.submitReplacement(target, newValue);
		} catch (RuntimeException e) {
			reconnectOnException(e);
		}
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		try {
			facade.submitConditionalReplacement(target, newValue, precondition, requiredValue);
		} catch (RuntimeException e) {
			reconnectOnException(e);
		}
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		try {
			facade.submitInitialization(target, newValue);
		} catch (RuntimeException e) {
			reconnectOnException(e);
		}
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		try {
			facade.submitDeletion(target);
		} catch (RuntimeException e) {
			reconnectOnException(e);
		}
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		try {
			facade.submitConditionalDeletion(target, precondition, requiredValue);
		} catch (RuntimeException e) {
			reconnectOnException(e);
		}
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		try {
			facade.flush();
		} catch (RuntimeException e) {
			reconnectOnException(e);
		}
	}

	@Override
	public void refurbish() {
		try {
			facade.refurbish();
		} catch (RuntimeException e) {
			reconnectOnException(e);
		}
	}

	@Override
	public void close() {
		facade.close();
	}

	private void reconnect() {
		throw new NotYetImplementedException();
	}

	private void reconnectOnException(RuntimeException e) {
		throw new NotYetImplementedException(e);
	}

}
