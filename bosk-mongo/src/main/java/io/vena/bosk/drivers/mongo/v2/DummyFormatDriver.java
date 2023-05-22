package io.vena.bosk.drivers.mongo.v2;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.vena.bosk.Entity;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.io.IOException;
import java.lang.reflect.Type;
import org.bson.Document;

class DummyFormatDriver<R extends Entity> implements FormatDriver<R> {
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

	@Override
	public StateAndMetadata<R> loadAllState() {
		return null;
	}

	@Override
	public void onEvent(ChangeStreamDocument<Document> event) {

	}

	@Override
	public void onException(Exception e) {

	}
}
