package io.vena.bosk.drivers.mongo.v2;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.lang.Nullable;
import io.vena.bosk.Bosk;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.Entity;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.drivers.mongo.BsonPlugin;
import io.vena.bosk.drivers.mongo.MongoDriverSettings;
import io.vena.bosk.drivers.mongo.v2.Formatter.DocumentFields;
import io.vena.bosk.exceptions.InvalidTypeException;
import io.vena.bosk.exceptions.NotYetImplementedException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static io.vena.bosk.drivers.mongo.v2.Formatter.REVISION_ONE;
import static io.vena.bosk.drivers.mongo.v2.Formatter.REVISION_ZERO;
import static io.vena.bosk.drivers.mongo.v2.Formatter.dottedFieldNameOf;
import static io.vena.bosk.drivers.mongo.v2.Formatter.enclosingReference;
import static io.vena.bosk.drivers.mongo.v2.Formatter.referenceTo;
import static java.lang.String.format;
import static java.util.Collections.newSetFromMap;
import static org.bson.BsonBoolean.FALSE;

final class SingleDocFormatDriver<R extends Entity> implements FormatDriver<R> {
	private final String description;
	private final MongoDriverSettings settings;
	private final Formatter formatter;
	private final MongoCollection<Document> collection;
	private final Reference<R> rootRef;
	private final String echoPrefix;
	private final BoskDriver<R> downstream;
	private final FlushLock flushLock;

	private volatile BsonInt64 revisionToSkip = null;

	static final BsonString DOCUMENT_ID = new BsonString("boskDocument");
	static final String COLLECTION_NAME = "boskCollection";

	SingleDocFormatDriver(Bosk<R> bosk, MongoCollection<Document> collection, MongoDriverSettings driverSettings, BsonPlugin bsonPlugin, BoskDriver<R> downstream) {
		this.description = SingleDocFormatDriver.class.getSimpleName() + ": " + driverSettings;
		this.settings = driverSettings;
		this.formatter = new Formatter(bosk, bsonPlugin);
		this.collection = collection;
		this.echoPrefix = bosk.instanceID().toString();
		this.rootRef = bosk.rootReference();
		this.downstream = downstream;
		this.flushLock = new FlushLock(settings);
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		LOGGER.debug("+ submitReplacement({})", target);
		doUpdate(replacementDoc(target, newValue), standardPreconditions(target));
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		LOGGER.debug("+ submitInitialization({})", target);
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
		LOGGER.debug("+ submitDeletion({})", target);
		if (target.path().isEmpty()) {
			throw new IllegalArgumentException("Can't delete the root of the bosk");
		} else {
			doUpdate(deletionDoc(target), standardPreconditions(target));
		}
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		LOGGER.debug("+ submitConditionalReplacement({}, {} = {})", target, precondition, requiredValue);
		doUpdate(
			replacementDoc(target, newValue),
			explicitPreconditions(target, precondition, requiredValue));
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		LOGGER.debug("+ submitConditionalDeletion({}, {} = {})", target, precondition, requiredValue);
		doUpdate(
			deletionDoc(target),
			explicitPreconditions(target, precondition, requiredValue));
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		LOGGER.debug("+ flush()");
		flushLock.awaitRevision(readRevisionNumber());
		downstream.flush();
	}

	@Override
	public void close() {
		LOGGER.debug("+ close()");
		throw new NotYetImplementedException();
	}

	@Override
	public StateAndMetadata<R> loadAllState() {
		throw new NotYetImplementedException();
	}

	@Override
	public void initializeCollection(StateAndMetadata<R> contents) {
		BsonValue initialState = formatter.object2bsonValue(contents.state, rootRef.targetType());
		BsonInt64 newRevision = new BsonInt64(1 + contents.revision.longValue());
		BsonDocument update = new BsonDocument("$set", initialDocument(initialState, newRevision));
		BsonDocument filter = documentFilter();
		UpdateOptions options = new UpdateOptions().upsert(true);
		LOGGER.debug("** Initial tenant upsert for {}", DOCUMENT_ID);
		LOGGER.trace("| Filter: {}", filter);
		LOGGER.trace("| Update: {}", update);
		LOGGER.trace("| Options: {}", options);
		UpdateResult result = collection.updateOne(filter, update, options);
		LOGGER.debug("| Result: {}", result);
	}

	@Override
	public void onEvent(ChangeStreamDocument<Document> event) {
		LOGGER.debug("# EVENT: {}", event);
		switch (event.getOperationType()) {
			case INSERT: case REPLACE:// Both of these represent replacing the whole document
				// getFullDocument is reliable for INSERT and REPLACE operations:
				//   https://docs.mongodb.com/v4.0/reference/change-events/#change-stream-output
				LOGGER.debug("| Replace document - IGNORE");
				//driver.submitReplacement(rootRef, document2object(event.getFullDocument().get(DocumentFields.root), rootRef));
				// TODO
				break;
			case UPDATE:
				UpdateDescription updateDescription = event.getUpdateDescription();
				if (updateDescription != null) {
					BsonInt64 revision = getRevisionFromEvent(event);
					if (shouldNotSkip(revision)) {
						replaceUpdatedFields(updateDescription.getUpdatedFields());
						deleteRemovedFields(updateDescription.getRemovedFields());
					}

					// Now that we've done everything else, we can record that we've processed the event
					flushLock.notifyRevision(revision);
				}
				break;
			default:
				throw new NotYetImplementedException("Unknown change stream event: " + event);
		}
	}

	private static BsonInt64 getRevisionFromEvent(ChangeStreamDocument<Document> event) {
		if (event == null) {
			return null;
		}
		UpdateDescription updateDescription = event.getUpdateDescription();
		if (updateDescription == null) {
			return null;
		}
		BsonDocument updatedFields = updateDescription.getUpdatedFields();
		if (updatedFields == null) {
			return null;
		}
		return updatedFields.getInt64(DocumentFields.revision.name(), null);
	}

	@Override
	public void onRevisionToSkip(BsonInt64 revision) {
		revisionToSkip = revision;
	}

	//
	// MongoDB helpers
	//

	/**
	 * @return Non-null revision number as per the database.
	 * If the database contains no revision number, returns {@link io.vena.bosk.drivers.mongo.v2.Formatter#REVISION_ZERO}.
	 */
	private BsonInt64 readRevisionNumber() {
		try {
			try (MongoCursor<Document> cursor = collection
				.find(DOCUMENT_FILTER).limit(1)
				.projection(fields(include(DocumentFields.revision.name())))
				.cursor()
			) {
				Document doc = cursor.next();
				Long result = doc.get(DocumentFields.revision.name(), Long.class);
				if (result == null) {
					// Document exists but has no revision field.
					// In that case, newer servers (including this one) will create the
					// the field upon initialization, and we're ok to wait for any old
					// revision number at all.
					return REVISION_ZERO;
				} else {
					return new BsonInt64(result);
				}
			}
		} catch (NoSuchElementException e) {
			// Document doesn't exist at all yet. We're ok to wait for any update at all.
			return REVISION_ZERO;
		}
	}

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
		return new BsonDocument("$inc", new BsonDocument(DocumentFields.revision.name(), REVISION_ONE));
	}

	private BsonDocument initialDocument(BsonValue initialState, BsonInt64 revision) {
		BsonDocument fieldValues = new BsonDocument("_id", DOCUMENT_ID);

		fieldValues.put(DocumentFields.path.name(), new BsonString("/"));
		fieldValues.put(DocumentFields.state.name(), initialState);
		fieldValues.put(DocumentFields.echo.name(), new BsonString(format("%s_9999", echoPrefix)));
		fieldValues.put(DocumentFields.revision.name(), revision);

		return fieldValues;
	}

	/**
	 * @return true if something changed
	 */
	private boolean doUpdate(BsonDocument updateDoc, BsonDocument filter) {
		LOGGER.debug("| Update: {}", updateDoc);
		if (settings.testing().eventDelayMS() < 0) {
			LOGGER.debug("| Sleeping");
			try {
				Thread.sleep(-settings.testing().eventDelayMS());
			} catch (InterruptedException e) {
				LOGGER.debug("| Interrupted");
			}
		}
		LOGGER.debug("| Filter: {}", filter);
		UpdateResult result = collection.updateOne(filter, updateDoc);
		LOGGER.debug("| Update result: {}", result);
		if (result.wasAcknowledged()) {
			assert result.getMatchedCount() <= 1;
			return result.getMatchedCount() >= 1;
		} else {
			LOGGER.error("Mongo write was not acknowledged.\n\tFilter: {}\n\tUpdate: {}\n\tResult: {}", filter, updateDoc, result);
			throw new NotYetImplementedException("Mongo write was not acknowledged");
		}
	}

	/**
	 * Call <code>downstream.{@link BoskDriver#submitReplacement submitReplacement}</code>
	 * for each updated field.
	 */
	private void replaceUpdatedFields(@Nullable BsonDocument updatedFields) {
		if (updatedFields != null) {
			for (Map.Entry<String, BsonValue> entry : updatedFields.entrySet()) {
				String dottedName = entry.getKey();
				if (dottedName.startsWith(DocumentFields.state.name())) {
					Reference<Object> ref;
					try {
						ref = referenceTo(dottedName, rootRef);
					} catch (InvalidTypeException e) {
						logNonexistentField(dottedName, e);
						continue;
					}
					LOGGER.debug("| Replace {}", ref);
					Object replacement = formatter.bsonValue2object(entry.getValue(), ref);
					downstream.submitReplacement(ref, replacement);
				}
			}
		}
	}

	private boolean shouldNotSkip(BsonInt64 revision) {
		return revision == null || revisionToSkip == null || revision.longValue() > revisionToSkip.longValue();
	}

	/**
	 * Call <code>downstream.{@link BoskDriver#submitDeletion submitDeletion}</code>
	 * for each removed field.
	 */
	private void deleteRemovedFields(@Nullable List<String> removedFields) {
		if (removedFields != null) {
			for (String dottedName : removedFields) {
				if (dottedName.startsWith(DocumentFields.state.name())) {
					Reference<Object> ref;
					try {
						ref = referenceTo(dottedName, rootRef);
					} catch (InvalidTypeException e) {
						logNonexistentField(dottedName, e);
						continue;
					}
					LOGGER.debug("| Delete {}", ref);
					downstream.submitDeletion(ref);
				}
			}
		}
	}

	private void logNonexistentField(String dottedName, InvalidTypeException e) {
		LOGGER.trace("Nonexistent field {}",  dottedName, e);
		if (LOGGER.isWarnEnabled() && ALREADY_WARNED.add(dottedName)) {
			LOGGER.warn("Ignoring updates of nonexistent field {}", dottedName);
		}
	}

	@Override
	public String toString() {
		return description;
	}

	private static final Set<String> ALREADY_WARNED = newSetFromMap(new ConcurrentHashMap<>());
	private static final BsonDocument DOCUMENT_FILTER = new BsonDocument("_id", DOCUMENT_ID);
	private static final Logger LOGGER = LoggerFactory.getLogger(SingleDocFormatDriver.class);
}
