package io.vena.bosk.drivers.mongo;

import com.mongodb.MongoClientSettings;
import io.vena.bosk.Bosk;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.Entity;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.drivers.mongo.modal.ChangeStreamListener;
import io.vena.bosk.drivers.mongo.modal.DisconnectedModeDriver;
import io.vena.bosk.drivers.mongo.modal.FutureMongoDriver;
import io.vena.bosk.drivers.mongo.modal.ModalDriverFacade;
import io.vena.bosk.drivers.mongo.modal.ReconnectingModeDriver;
import io.vena.bosk.exceptions.InvalidTypeException;
import io.vena.bosk.exceptions.NotYetImplementedException;
import java.io.IOException;
import java.lang.reflect.Type;

import static io.vena.bosk.drivers.mongo.SingleDocumentMongoDriver.validateMongoClientSettings;

public class MainMongoDriver<R extends Entity> implements MongoDriver<R> {
	private final BoskDriver<R> downstream;
	private final ModalDriverFacade<R> facade;
	private final ChangeStreamListener listener;

	private final ReconnectingModeDriver<R> reconnectingModeDriver;
	private final DisconnectedModeDriver<R> disconnectedModeDriver;

	public MainMongoDriver(Bosk<R> bosk, MongoClientSettings clientSettings, MongoDriverSettings driverSettings, BsonPlugin bsonPlugin, BoskDriver<R> downstream) {
		validateMongoClientSettings(clientSettings);
		FutureMongoDriver<R> future;
		this.downstream = downstream;
		this.reconnectingModeDriver = new ReconnectingModeDriver<>();
		this.disconnectedModeDriver = new DisconnectedModeDriver<>(downstream);
		this.facade = new ModalDriverFacade<>(reconnectingModeDriver);
		this.listener = new ChangeStreamListener(this::reconnect);
	}

	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		// TODO: This is only appropriate as long as we are initially reconnecting
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
