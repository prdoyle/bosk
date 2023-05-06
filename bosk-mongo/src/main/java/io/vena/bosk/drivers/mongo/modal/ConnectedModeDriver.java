package io.vena.bosk.drivers.mongo.modal;

import io.vena.bosk.Entity;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.drivers.mongo.MongoDriver;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.io.IOException;
import java.lang.reflect.Type;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ConnectedModeDriver<R extends Entity> implements MongoDriver<R> {
	private final MongoDriver<R> downstream;

	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		return downstream.initialRoot(rootType);
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		downstream.submitReplacement(target, newValue);
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		downstream.submitConditionalReplacement(target, newValue, precondition, requiredValue);
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		downstream.submitInitialization(target, newValue);
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		downstream.submitDeletion(target);
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		downstream.submitConditionalDeletion(target, precondition, requiredValue);
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		downstream.flush();
	}

	@Override
	public void refurbish() {
		downstream.refurbish();
	}

	@Override
	public void close() {
		downstream.close();
	}
}
