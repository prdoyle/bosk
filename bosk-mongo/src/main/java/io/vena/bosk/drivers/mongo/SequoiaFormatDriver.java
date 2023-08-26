package io.vena.bosk.drivers.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.mongodb.client.result.UpdateResult;
import io.vena.bosk.Bosk;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.StateTreeNode;
import io.vena.bosk.drivers.mongo.Formatter.DocumentFields;
import io.vena.bosk.drivers.mongo.MongoDriverSettings.ManifestMode;
import java.io.IOException;
import java.util.NoSuchElementException;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mongodb.ReadConcern.LOCAL;
import static com.mongodb.client.model.changestream.OperationType.INSERT;
import static io.vena.bosk.drivers.mongo.Formatter.REVISION_ZERO;
import static io.vena.bosk.drivers.mongo.Formatter.dottedFieldNameOf;
import static io.vena.bosk.drivers.mongo.Formatter.enclosingReference;
import static io.vena.bosk.drivers.mongo.MainDriver.MANIFEST_ID;
import static java.util.Objects.requireNonNull;
import static org.bson.BsonBoolean.FALSE;

/**
 * A {@link FormatDriver} that stores the entire bosk state in a single document.
 */
final class SequoiaFormatDriver<R extends StateTreeNode> extends AbstractFormatDriver<R> {

	private volatile BsonInt64 revisionToSkip = null;


	SequoiaFormatDriver(
		Bosk<R> bosk,
		MongoCollection<Document> collection,
		MongoDriverSettings driverSettings,
		BsonPlugin bsonPlugin,
		FlushLock flushLock,
		BoskDriver<R> downstream
	) {
		super(
			SequoiaFormatDriver.class.getSimpleName() + ": " + driverSettings,
			collection,
			driverSettings,
			new Formatter(bosk, bsonPlugin),
			bosk.rootReference(),
			downstream,
			flushLock,
			Manifest.forSequoia()
		);
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		doUpdate(replacementDoc(target, newValue), standardPreconditions(target));
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		BsonDocument filter = standardPreconditions(target);
		filter.put(dottedFieldNameOf(target, rootRef), new BsonDocument("$exists", FALSE));
		if (doUpdate(replacementDoc(target, newValue), filter)) {
			LOGGER.debug("| Object initialized");
		} else {
			LOGGER.debug("| No update");
		}
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		doUpdate(deletionDoc(target), standardPreconditions(target));
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		doUpdate(
			replacementDoc(target, newValue),
			explicitPreconditions(target, precondition, requiredValue));
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		doUpdate(
			deletionDoc(target),
			explicitPreconditions(target, precondition, requiredValue));
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		flushLock.awaitRevision(readRevisionNumber());
		LOGGER.debug("| Flush downstream");
		downstream.flush();
	}

	@Override
	public void close() {
		LOGGER.debug("+ close()");
		flushLock.close();
	}

	@Override
	public StateAndMetadata<R> loadAllState() throws IOException, UninitializedCollectionException {
		try (MongoCursor<Document> cursor = collection
			.withReadConcern(LOCAL) // The revision field needs to be the latest
			.find(documentFilter())
			.limit(1)
			.cursor()
		) {
			Document document = cursor.next();
			Document state = document.get(DocumentFields.state.name(), Document.class);
			Long revision = document.get(DocumentFields.revision.name(), 0L);
			if (state == null) {
				throw new IOException("No existing state in document");
			} else {
				R root = formatter.document2object(state, rootRef);
				BsonInt64 rev = new BsonInt64(revision);
				return new StateAndMetadata<>(root, rev);
			}
		} catch (NoSuchElementException e) {
			throw new UninitializedCollectionException("No existing document", e);
		}

	}

	@Override
	public void initializeCollection(StateAndMetadata<R> priorContents) {
		BsonValue initialState = formatter.object2bsonValue(priorContents.state, rootRef.targetType());
		BsonInt64 newRevision = new BsonInt64(1 + priorContents.revision.longValue());
		BsonDocument update = new BsonDocument("$set", initialDocument(initialState, newRevision));
		BsonDocument filter = documentFilter();
		UpdateOptions options = new UpdateOptions().upsert(true);
		LOGGER.debug("** Initial tenant upsert for {}", DOCUMENT_ID);
		LOGGER.trace("| Filter: {}", filter);
		LOGGER.trace("| Update: {}", update);
		LOGGER.trace("| Options: {}", options);
		UpdateResult result = collection.updateOne(filter, update, options);
		LOGGER.debug("| Result: {}", result);
		if (settings.experimental().manifestMode() == ManifestMode.ENABLED) {
			writeManifest();
		}
	}

	/**
	 * We're required to cope with anything we might ourselves do in {@link #initializeCollection}.
	 */
	@Override
	public void onEvent(ChangeStreamDocument<Document> event) throws UnprocessableEventException {
		if (settings.experimental().manifestMode() == ManifestMode.ENABLED) {
			if (event.getDocumentKey() == null) {
				throw new UnprocessableEventException("Null document key", event.getOperationType());
			}
			if (MANIFEST_ID.equals(event.getDocumentKey().get("_id"))) {
				onManifestEvent(event);
				return;
			}
		}
		if (!DOCUMENT_FILTER.equals(event.getDocumentKey())) {
			LOGGER.debug("Ignoring event for unrecognized document key: {}", event.getDocumentKey());
			return;
		}
		switch (event.getOperationType()) {
			case INSERT: case REPLACE: {
				Document fullDocument = event.getFullDocument();
				if (fullDocument == null) {
					throw new UnprocessableEventException("Missing fullDocument", event.getOperationType());
				}
				BsonInt64 revision = getRevisionFromFullDocumentEvent(fullDocument);
				Document state = fullDocument.get(DocumentFields.state.name(), Document.class);
				if (state == null) {
					throw new UnprocessableEventException("Missing state field", event.getOperationType());
				}
				R newRoot = formatter.document2object(state, rootRef);
				LOGGER.debug("| Replace {}", rootRef);
				downstream.submitReplacement(rootRef, newRoot);
				flushLock.finishedRevision(revision);
			} break;
			case UPDATE: {
				UpdateDescription updateDescription = event.getUpdateDescription();
				if (updateDescription != null) {
					BsonInt64 revision = getRevisionFromUpdateEvent(event);
					if (shouldNotSkip(revision)) {
						replaceUpdatedFields(updateDescription.getUpdatedFields());
						deleteRemovedFields(updateDescription.getRemovedFields(), event.getOperationType());
					}
					flushLock.finishedRevision(revision);
				}
			} break;
			case DELETE: {
				LOGGER.debug("Document containing revision field has been deleted; assuming revision=0");
				flushLock.finishedRevision(REVISION_ZERO);
			} break;
			default: {
				throw new UnprocessableEventException("Cannot process event", event.getOperationType());
			}
		}
	}

	/**
	 * We're required to cope with anything we might ourselves do in {@link #initializeCollection},
	 * but outside that, we want to be as strict as we can
	 * so incompatible database changes don't go unnoticed.
	 */
	private static void onManifestEvent(ChangeStreamDocument<Document> event) throws UnprocessableEventException {
		if (event.getOperationType() == INSERT) {
			Document manifest = requireNonNull(event.getFullDocument());
			try {
				MainDriver.validateManifest(manifest);
			} catch (UnrecognizedFormatException e) {
				throw new UnprocessableEventException("Invalid manifest", e, event.getOperationType());
			}
			if (!new Document().equals(manifest.get("sequoia"))) {
				throw new UnprocessableEventException("Unexpected value in manifest \"sequoia\" field: " + manifest.get("sequoia"), event.getOperationType());
			}
		} else {
			throw new UnprocessableEventException("Unexpected change to manifest document", event.getOperationType());
		}
		LOGGER.debug("Ignoring benign manifest change event");
	}

	@Override
	public void onRevisionToSkip(BsonInt64 revision) {
		revisionToSkip = revision;
		flushLock.finishedRevision(revision);
	}

	//
	// MongoDB helpers
	//

	private BsonDocument documentFilter() {
		return new BsonDocument("_id", DOCUMENT_ID);
	}

	private <T> BsonDocument standardPreconditions(Reference<T> target) {
		BsonDocument filter = documentFilter();
		if (!target.path().isEmpty()) {
			String enclosingObjectKey = dottedFieldNameOf(enclosingReference(target), rootRef);
			BsonDocument condition = new BsonDocument("$type", new BsonString("object"));
			filter.put(enclosingObjectKey, condition);
			LOGGER.debug("| Precondition: {} {}", enclosingObjectKey, condition);
		}
		return filter;
	}

	private <T> BsonDocument explicitPreconditions(Reference<T> target, Reference<Identifier> preconditionRef, Identifier requiredValue) {
		BsonDocument filter = standardPreconditions(target);
		BsonDocument precondition = new BsonDocument("$eq", new BsonString(requiredValue.toString()));
		filter.put(dottedFieldNameOf(preconditionRef, rootRef), precondition);
		return filter;
	}

	private <T> BsonDocument replacementDoc(Reference<T> target, T newValue) {
		String key = dottedFieldNameOf(target, rootRef);
		BsonValue value = formatter.object2bsonValue(newValue, target.targetType());
		LOGGER.debug("| Set field {}: {}", key, value);
		return updateDoc()
			.append("$set", new BsonDocument(key, value));
	}

	private <T> BsonDocument deletionDoc(Reference<T> target) {
		String key = dottedFieldNameOf(target, rootRef);
		LOGGER.debug("| Unset field {}", key);
		return updateDoc().append("$unset", new BsonDocument(key, new BsonNull())); // Value is ignored
	}

	private BsonDocument updateDoc() {
		return new BsonDocument("$inc", new BsonDocument(DocumentFields.revision.name(), new BsonInt64(1)));
	}

	private BsonDocument initialDocument(BsonValue initialState, BsonInt64 revision) {
		BsonDocument fieldValues = new BsonDocument("_id", DOCUMENT_ID);

		fieldValues.put(DocumentFields.path.name(), new BsonString("/"));
		fieldValues.put(DocumentFields.state.name(), initialState);
		fieldValues.put(DocumentFields.revision.name(), revision);

		return fieldValues;
	}

	private boolean shouldNotSkip(BsonInt64 revision) {
		return revision == null || revisionToSkip == null || revision.longValue() > revisionToSkip.longValue();
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(SequoiaFormatDriver.class);
}
