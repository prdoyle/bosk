package io.vena.bosk.drivers.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.lang.Nullable;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.Reference;
import io.vena.bosk.StateTreeNode;
import io.vena.bosk.drivers.mongo.Formatter.DocumentFields;
import io.vena.bosk.exceptions.FlushFailureException;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.RequiredArgsConstructor;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mongodb.ReadConcern.LOCAL;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static io.vena.bosk.drivers.mongo.Formatter.REVISION_ZERO;
import static io.vena.bosk.drivers.mongo.Formatter.referenceTo;
import static io.vena.bosk.drivers.mongo.MainDriver.MANIFEST_ID;
import static java.util.Collections.newSetFromMap;

@RequiredArgsConstructor
abstract class AbstractFormatDriver<R extends StateTreeNode> implements FormatDriver<R> {
	protected final String description;
	protected final MongoCollection<Document> collection;
	protected final MongoDriverSettings settings;
	protected final Formatter formatter;
	protected final Reference<R> rootRef;
	protected final BoskDriver<R> downstream;
	protected final FlushLock flushLock;
	protected final Manifest manifest;

	protected static BsonInt64 getRevisionFromUpdateEvent(ChangeStreamDocument<Document> event) {
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

	protected BsonInt64 getRevisionFromFullDocumentEvent(Document fullDocument) {
		if (fullDocument == null) {
			return null;
		}
		Long revision = fullDocument.getLong(DocumentFields.revision.name());
		if (revision == null) {
			return null;
		} else {
			return new BsonInt64(revision);
		}
	}

	/**
	 * @return Non-null revision number as per the database.
	 * If the database contains no revision number, returns {@link Formatter#REVISION_ZERO}.
	 */
	protected BsonInt64 readRevisionNumber() throws FlushFailureException {
		LOGGER.debug("readRevisionNumber");
		try {
			try (MongoCursor<Document> cursor = collection
				.withReadConcern(LOCAL) // The revision field needs to be the latest
				.find(SequoiaFormatDriver.DOCUMENT_FILTER)
				.limit(1)
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
					LOGGER.debug("No revision field; assuming {}", REVISION_ZERO.longValue());
					return REVISION_ZERO;
				} else {
					LOGGER.debug("Read revision {}", result);
					return new BsonInt64(result);
				}
			}
		} catch (NoSuchElementException e) {
			LOGGER.debug("Document is missing", e);
			throw new RevisionFieldDisruptedException(e);
		} catch (RuntimeException e) {
			LOGGER.debug("readRevisionNumber failed", e);
			throw new FlushFailureException(e);
		}
	}

	protected void logNonexistentField(String dottedName, InvalidTypeException e) {
		LOGGER.trace("Nonexistent field {}",  dottedName, e);
		if (LOGGER.isWarnEnabled() && ALREADY_WARNED.add(dottedName)) {
			LOGGER.warn("Ignoring updates of nonexistent field {}", dottedName);
		}
	}

	protected void writeManifest() {
		BsonDocument doc = new BsonDocument("_id", MANIFEST_ID);
		doc.putAll((BsonDocument) formatter.object2bsonValue(manifest, Manifest.class));
		BsonDocument update = new BsonDocument("$set", doc);
		BsonDocument filter = new BsonDocument("_id", MANIFEST_ID);
		UpdateOptions options = new UpdateOptions().upsert(true);
		LOGGER.debug("| Initial manifest: {}", doc);
		UpdateResult result = collection.updateOne(filter, update, options);
		LOGGER.debug("| Manifest result: {}", result);
	}

	/**
	 * @return true if something changed
	 */
	protected boolean doUpdate(BsonDocument updateDoc, BsonDocument filter) {
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
			LOGGER.error("MongoDB write was not acknowledged");
			LOGGER.trace("Details of MongoDB write not acknowledged:\n\tFilter: {}\n\tUpdate: {}\n\tResult: {}", filter, updateDoc, result);
			throw new IllegalStateException("Mongo write was not acknowledged: " + result);
		}
	}

	/**
	 * Call <code>downstream.{@link BoskDriver#submitReplacement submitReplacement}</code>
	 * for each updated field.
	 */
	protected void replaceUpdatedFields(@Nullable BsonDocument updatedFields) {
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

	/**
	 * Call <code>downstream.{@link BoskDriver#submitDeletion submitDeletion}</code>
	 * for each removed field.
	 */
	protected void deleteRemovedFields(@Nullable List<String> removedFields, OperationType operationType) throws UnprocessableEventException {
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
				} else {
					throw new UnprocessableEventException("Deletion of metadata field " + dottedName, operationType);
				}
			}
		}
	}

	@Override
	public String toString() {
		return description;
	}

	static final BsonString DOCUMENT_ID = new BsonString("boskDocument");
	protected static final BsonDocument DOCUMENT_FILTER = new BsonDocument("_id", DOCUMENT_ID);
	private static final Set<String> ALREADY_WARNED = newSetFromMap(new ConcurrentHashMap<>());
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFormatDriver.class);

}
