package io.vena.bosk.drivers.mongo.v2;

import com.mongodb.client.MongoClient;
import io.vena.bosk.Entity;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.drivers.mongo.MongoDriver;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.io.IOException;
import java.lang.reflect.Type;
import lombok.RequiredArgsConstructor;

import static lombok.AccessLevel.PACKAGE;

@RequiredArgsConstructor(access = PACKAGE)
public class MainDriver<R extends Entity> implements MongoDriver<R> {
	private final MongoClient client;
	private final ChangeEventReceiver listener;

	private volatile FormatDriver<R> formatDriver;

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
