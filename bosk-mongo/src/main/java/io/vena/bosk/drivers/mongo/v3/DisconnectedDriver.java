package io.vena.bosk.drivers.mongo.v3;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.vena.bosk.Entity;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.exceptions.InitializationFailureException;
import io.vena.bosk.exceptions.NotYetImplementedException;
import java.io.IOException;
import lombok.RequiredArgsConstructor;
import org.bson.Document;

@RequiredArgsConstructor
public class DisconnectedDriver<R extends Entity> implements FormatDriver<R> {
	private final String reason;
	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		throw new NotYetImplementedException();
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		throw new NotYetImplementedException();
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		throw new NotYetImplementedException();
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		throw new NotYetImplementedException();
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		throw new NotYetImplementedException();
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		throw new NotYetImplementedException();
	}

	@Override
	public void close() {
		throw new NotYetImplementedException();
	}

	@Override
	public void onEvent(ChangeStreamDocument<Document> event) throws UnprocessableEventException {
		throw new NotYetImplementedException();
	}

	@Override
	public StateAndMetadata<R> loadAllState() throws IOException, UninitializedCollectionException {
		throw new NotYetImplementedException();
	}

	@Override
	public void initializeCollection(StateAndMetadata<R> priorContents) throws InitializationFailureException {
		throw new NotYetImplementedException();
	}
}
