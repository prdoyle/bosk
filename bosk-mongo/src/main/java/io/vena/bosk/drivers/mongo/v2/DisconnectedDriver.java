package io.vena.bosk.drivers.mongo.v2;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.vena.bosk.Entity;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.exceptions.FlushFailureException;
import io.vena.bosk.exceptions.InitializationFailureException;
import io.vena.bosk.exceptions.InvalidTypeException;
import io.vena.bosk.exceptions.NotYetImplementedException;
import java.io.IOException;
import java.lang.reflect.Type;
import org.bson.BsonInt64;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.jvm.hotspot.utilities.AssertionFailure;

class DisconnectedDriver<R extends Entity> implements FormatDriver<R> {
	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		throw new InitializationFailureException("Disconnected");
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		throw disconnected();
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		throw disconnected();
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		throw disconnected();
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		throw disconnected();
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		throw disconnected();
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		throw new FlushFailureException("Disconnected");
	}

	@Override
	public void refurbish() {
		throw disconnected();
	}

	@Override
	public void close() { }

	@Override
	public StateAndMetadata<R> loadAllState() {
		throw disconnected();
	}

	@Override
	public void onRevisionToSkip(BsonInt64 revision) {
		throw new AssertionFailure("Resynchronization should not tell DisconnectedDriver to skip a revision");
	}

	private RuntimeException disconnected() {
		return new NotYetImplementedException("Disconnected");
	}

	@Override
	public void onEvent(ChangeStreamDocument<Document> event) {
		LOGGER.info("Event received in disconnected mode: {} {}", event.getOperationType(), event.getResumeToken());
	}

	@Override
	public void onException(Exception e) {
		LOGGER.info("Exception occurred in disconnected mode", e);
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(DisconnectedDriver.class);
}
