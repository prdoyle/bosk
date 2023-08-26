package io.vena.bosk.drivers.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.vena.bosk.Bosk;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.EnumerableByIdentifier;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.StateTreeNode;
import io.vena.bosk.exceptions.InitializationFailureException;
import java.io.IOException;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.bson.Document;

final class PandoFormatDriver<R extends StateTreeNode> extends AbstractFormatDriver<R> {
	private final BsonSurgeon surgeon;

	public PandoFormatDriver(
		Bosk<R> bosk,
		MongoCollection<Document> collection,
		MongoDriverSettings driverSettings,
		BsonPlugin bsonPlugin,
		FlushLock flushLock,
		BoskDriver<R> downstream,
		List<Reference<? extends EnumerableByIdentifier<?>>> separateCollections)
	{
		super(
			PandoFormatDriver.class.getSimpleName() + ": " + driverSettings,
			collection,
			driverSettings,
			new Formatter(bosk, bsonPlugin),
			bosk.rootReference(),
			downstream,
			flushLock,
			Manifest.forPando()
		);
		this.surgeon = new BsonSurgeon(separateCollections); // TODO: From driverSettings
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		BsonValue value = formatter.object2bsonValue(newValue, target.targetType());
		if (value instanceof BsonDocument) {
			List<BsonDocument> parts = surgeon.scatter(rootRef, target, (BsonDocument) value);

		}
		doUpdate(replacementDoc(target, newValue), standardPreconditions(target));
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
	public void onEvent(ChangeStreamDocument<Document> event) throws UnprocessableEventException {

	}

	@Override
	public void onRevisionToSkip(BsonInt64 revision) {

	}

	@Override
	public StateAndMetadata<R> loadAllState() throws IOException, UninitializedCollectionException {
		return null;
	}

	@Override
	public void initializeCollection(StateAndMetadata<R> priorContents) throws InitializationFailureException {

	}

	@Override
	public void close() {

	}
}
