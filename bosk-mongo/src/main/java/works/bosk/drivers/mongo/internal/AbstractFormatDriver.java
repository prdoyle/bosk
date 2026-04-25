package works.bosk.drivers.mongo.internal;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.lang.Nullable;
import jakarta.annotation.Nonnull;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import works.bosk.BoskContext;
import works.bosk.BoskDriver;
import works.bosk.MapValue;
import works.bosk.Reference;
import works.bosk.RootReference;
import works.bosk.StateTreeNode;
import works.bosk.drivers.mongo.internal.BsonFormatter.DocumentFields;
import works.bosk.drivers.mongo.status.BsonComparator;
import works.bosk.drivers.mongo.status.MongoStatus;
import works.bosk.drivers.mongo.status.StateStatus;
import works.bosk.exceptions.FlushFailureException;
import works.bosk.exceptions.InvalidTypeException;

import static com.mongodb.ReadConcern.LOCAL;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.changestream.OperationType.INSERT;
import static com.mongodb.client.model.changestream.OperationType.REPLACE;
import static java.util.Collections.newSetFromMap;
import static java.util.Objects.requireNonNull;
import static works.bosk.drivers.mongo.internal.BsonFormatter.dottedFieldNameOf;
import static works.bosk.drivers.mongo.internal.BsonFormatter.referenceTo;
import static works.bosk.drivers.mongo.internal.Formatter.REVISION_BEFORE_ANY;
import static works.bosk.drivers.mongo.internal.Formatter.REVISION_ZERO;
import static works.bosk.drivers.mongo.internal.MainDriver.MANIFEST_ID;

/**
 * Shared code for format drivers, whose behaviour can be complex and subtle,
 * so we definitely want to avoid duplicating it between Sequoia and Pando.
 * <p>
 * This is not a fully general base class for format drivers,
 * but rather a common foundation for Sequoia and Pando specifically.
 * If we were to add a third format, this base class might need some
 * refactoring, or it might not be useful at all.
 *
 * @param <R>
 */
abstract non-sealed class AbstractFormatDriver<R extends StateTreeNode> implements FormatDriver<R> {
	final RootReference<R> rootRef;
	final BoskContext context;
	final Formatter formatter;
	final TransactionalCollection collection;
	final BoskDriver downstream;
	final FlushLock flushLock;

	public AbstractFormatDriver(
		RootReference<R> rootRef,
		BoskContext context,
		Formatter formatter,
		TransactionalCollection collection,
		BoskDriver downstream,
		long flushTimeoutMS
	) {
		this.rootRef = rootRef;
		this.context = context;
		this.formatter = formatter;
		this.collection = collection;
		this.downstream = downstream;

		// The proper revision number will be established by loadAllState or initializeCollection.
		// The value we use here doesn't matter a lot, provided that either loadAllState or
		// initializeCollection is called before the first flush (a condition that is trivially
		// satisfied if this driver is discarded before the first flush).
		// In the meantime, let's use a value guaranteed to be less than any real revision number
		// on the basis that blocking is safer than accidentally proceeding without waiting on a flush.
		this.flushLock = new FlushLock(REVISION_BEFORE_ANY.longValue(), flushTimeoutMS);
	}

	/**
	 * Low-level read of the database contents, with only the minimum interpretation
	 * necessary to determine what the various parts correspond to.
	 *
	 * @return the contents of the database; fields of the returned
	 * record can be null if they don't exist in the database.
	 */
	abstract BsonStateAndMetadata loadBsonStateAndMetadata() throws UninitializedCollectionException;

	/**
	 * @return MongoDB filter that identifies the document containing
	 * the state tree's root node as well as the revision number.
	 */
	abstract BsonDocument rootDocumentFilter();

	@Override
	public MongoStatus readStatus() {
		try {
			BsonStateAndMetadata dbContents = loadBsonStateAndMetadata();
			BsonDocument loadedBsonState = dbContents.state;
			BsonValue inMemoryState = formatter.object2bsonValue(rootRef.value(), rootRef.targetType());
			BsonComparator comp = new BsonComparator();
			return new MongoStatus(
				null,
				null, // MainDriver should fill this in
				new StateStatus(
					dbContents.revision.longValue(),
					formatter.bsonValueBinarySize(loadedBsonState),
					comp.difference(inMemoryState, loadedBsonState)
				)
			);
		} catch (UninitializedCollectionException e) {
			return new MongoStatus(
				e.toString(),
				null,
				null
			);
		}
	}

	@Override
	public StateAndMetadata<R> loadAllState() throws IOException, UninitializedCollectionException {
		BsonStateAndMetadata bsonStateAndMetadata = loadBsonStateAndMetadata();
		if (bsonStateAndMetadata.state() == null) {
			throw new IOException("No existing state in document");
		}

		R root = formatter.document2object(bsonStateAndMetadata.state(), rootRef);
		BsonInt64 revision = bsonStateAndMetadata.revision() == null ? REVISION_ZERO : bsonStateAndMetadata.revision();
		MapValue<String> diagnosticAttributes = bsonStateAndMetadata.diagnosticAttributes() == null
			? MapValue.empty() // It's not clear what missing attributes mean, but using null here would have the effect of leaving the old attributes in place, which seems flaky
			: formatter.decodeDiagnosticAttributes(bsonStateAndMetadata.diagnosticAttributes());

		// Update the state that we "know about"
		flushLock.finishedRevision(revision);

		return new StateAndMetadata<>(root, revision, diagnosticAttributes);
	}

	protected BsonDocument blankUpdateDoc() {
		return new BsonDocument()
			.append("$inc", new BsonDocument(DocumentFields.revision.name(), new BsonInt64(1)))
			.append("$set", new BsonDocument()
				.append(
					DocumentFields.diagnostics.name(),
					formatter.encodeDiagnostics(context.getAttributes())
				)
				.append(
					DocumentFields.tenant.name(),
					formatter.encodeTenant(context.getEstablishedTenant())
				)
			);
	}

	protected void logNonexistentField(String dottedName, InvalidTypeException e) {
		LOGGER.trace("Nonexistent field {}", dottedName, e);
		if (LOGGER.isWarnEnabled() && ALREADY_WARNED.add(dottedName)) {
			LOGGER.warn("Ignoring updates of nonexistent field {}", dottedName);
		}
	}

	protected <T> BsonDocument replacementDoc(Reference<T> target, BsonValue value, Reference<?> startingRef) {
		String key = dottedFieldNameOf(target, startingRef);
		LOGGER.debug("| Set field {}: {}", key, value);
		BsonDocument result = blankUpdateDoc();
		result.compute("$set", (_, existing) -> {
			if (existing == null) {
				return new BsonDocument(key, value);
			} else {
				return existing.asDocument().append(key, value);
			}
		});
		return result;
	}

	protected <T> BsonDocument deletionDoc(Reference<T> target, Reference<?> startingRef) {
		String key = dottedFieldNameOf(target, startingRef);
		LOGGER.debug("| Unset field {}", key);
		return blankUpdateDoc().append("$unset", new BsonDocument(key, BsonNull.VALUE));
	}

	protected boolean shouldSkip(BsonInt64 revision) {
		return revision != null && flushLock.alreadySeen(revision);
	}

	/**
	 * We're required to cope with anything we might ourselves do in {@link #initializeCollection},
	 * but outside that, we want to be as strict as possible
	 * so incompatible database changes don't go unnoticed.
	 */
	protected void validateManifestEvent(ChangeStreamDocument<BsonDocument> event, Manifest effectiveManifest) throws UnprocessableEventException {
		LOGGER.debug("onManifestEvent({})", event.getOperationType().name());
		if (event.getOperationType() == INSERT || event.getOperationType() == REPLACE) {
			BsonDocument manifestDoc = requireNonNull(event.getFullDocument());
			Manifest manifest;
			try {
				manifest = formatter.decodeManifest(manifestDoc);
			} catch (UnrecognizedFormatException e) {
				throw new UnprocessableEventException("Invalid manifest", e, event.getOperationType());
			}
			if (!manifest.equals(effectiveManifest)) {
				throw new UnprocessableEventException("Manifest indicates format has changed", event.getOperationType());
			}
		} else {
			// We always use INSERT/REPLACE to update the manifest;
			// anything else is unexpected.
			throw new UnprocessableEventException("Unexpected change to manifest document", event.getOperationType());
		}
		LOGGER.debug("Ignoring benign manifest change event");
	}

	/**
	 * @return Revision number as per the database.
	 * If the database contains no revision number, returns {@link Formatter#REVISION_ZERO}.
	 */
	protected @Nonnull BsonInt64 readRevisionNumber() throws FlushFailureException {
		LOGGER.debug("readRevisionNumber");
		try {
			try (MongoCursor<BsonDocument> cursor = collection
				.withReadConcern(LOCAL)
				.find(rootDocumentFilter())
				.limit(1)
				.projection(fields(include(DocumentFields.revision.name())))
				.cursor()
			) {
				BsonDocument doc = cursor.next();
				BsonInt64 result = doc.getInt64(DocumentFields.revision.name(), null);
				if (result == null) {
					// TODO: Is this still relevant? Seems like legacy
					LOGGER.debug("No revision field; assuming {}", REVISION_ZERO.longValue());
					return REVISION_ZERO;
				} else {
					LOGGER.debug("Read revision {}", result.longValue());
					return result;
				}
			}
		} catch (NoSuchElementException e) {
			LOGGER.debug("Document is missing", e);
			throw new RevisionFieldDisruptedException("State document is missing", e);
		} catch (RuntimeException e) {
			LOGGER.debug("readRevisionNumber failed", e);
			throw new FlushFailureException(e);
		}
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

	protected void writeManifest(Manifest manifest) {
		BsonDocument doc = new BsonDocument("_id", requireNonNull(MANIFEST_ID));
		doc.putAll((BsonDocument) formatter.object2bsonValue(manifest, Manifest.class));
		BsonDocument filter = new BsonDocument("_id", MANIFEST_ID);
		LOGGER.debug("| Initial manifest: {}", doc);
		ReplaceOptions options = new ReplaceOptions().upsert(true);
		UpdateResult result = collection.replaceOne(filter, doc, options);
		LOGGER.debug("| Manifest result: {}", result);
	}

	protected BsonDocument initialDocument(BsonValue initialState, BsonInt64 revision, BsonString documentId) {
		BsonDocument fieldValues = new BsonDocument("_id", documentId);

		fieldValues.put(DocumentFields.path.name(), new BsonString("/"));
		fieldValues.put(DocumentFields.state.name(), initialState);
		fieldValues.put(DocumentFields.revision.name(), revision);
		fieldValues.put(DocumentFields.tenant.name(), formatter.encodeMaybeTenant(context.getTenant()));
		fieldValues.put(DocumentFields.diagnostics.name(), formatter.encodeDiagnostics(context.getAttributes()));

		return fieldValues;
	}

	protected boolean isManifestID(BsonValue documentId) {
		return MANIFEST_ID.equals(documentId);
	}

	/**
	 * Low-level version of {@link StateAndMetadata}.
	 */
	record BsonStateAndMetadata(
		BsonDocument state,
		BsonInt64 revision,
		BsonDocument diagnosticAttributes
	){}

	/**
	 * Calls <code>downstream.{@link BoskDriver#submitReplacement submitReplacement}</code>
	 * for each updated field in <code>updatedFields</code> whose key starts with {@code state.}.
	 *
	 * @param rootRef the reference to interpret dotted names against
	 * @param updatedFields the BSON document containing updated field names and values
	 */
	protected void replaceUpdatedFields(Reference<?> rootRef, @Nullable BsonDocument updatedFields) {
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
	 * Calls <code>downstream.{@link BoskDriver#submitDeletion submitDeletion}</code>
	 * for each removed field in <code>removedFields</code> whose name starts with {@code state.}.
	 *
	 * @param rootRef the reference to interpret dotted names against
	 * @param removedFields the list of removed field names
	 * @param operationType the type of change stream event (used in error messages)
	 * @throws UnprocessableEventException if a removed field is not a state field
	 */
	protected void deleteRemovedFields(Reference<?> rootRef, @Nullable List<String> removedFields, OperationType operationType) throws UnprocessableEventException {
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

	private static final Set<String> ALREADY_WARNED = newSetFromMap(new ConcurrentHashMap<>());
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFormatDriver.class);

}
