package io.vena.bosk.drivers.mongo;

import com.mongodb.ClientSessionOptions;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.lang.Nullable;
import io.vena.bosk.Bosk;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.Entity;
import io.vena.bosk.EnumerableByIdentifier;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.RootReference;
import io.vena.bosk.StateTreeNode;
import io.vena.bosk.drivers.mongo.Formatter.DocumentFields;
import io.vena.bosk.exceptions.FlushFailureException;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mongodb.ReadConcern.LOCAL;
import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.changestream.OperationType.INSERT;
import static io.vena.bosk.Path.parseParameterized;
import static io.vena.bosk.drivers.mongo.BsonSurgeon.containerSegments;
import static io.vena.bosk.drivers.mongo.Formatter.REVISION_ZERO;
import static io.vena.bosk.drivers.mongo.Formatter.dottedFieldNameOf;
import static io.vena.bosk.drivers.mongo.Formatter.enclosingReference;
import static io.vena.bosk.drivers.mongo.MainDriver.MANIFEST_ID;
import static io.vena.bosk.drivers.mongo.MongoDriverSettings.ManifestMode.CREATE_IF_ABSENT;
import static io.vena.bosk.util.Classes.enumerableByIdentifier;
import static java.util.Collections.newSetFromMap;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.bson.BsonBoolean.FALSE;

/**
 * A {@link FormatDriver} that stores the entire bosk state in a single document.
 */
final class PandoFormatDriver<R extends StateTreeNode> implements FormatDriver<R> {
	private final String description;
	private final PandoFormat format;
	private final MongoDriverSettings settings;
	private final Formatter formatter;
	private final MongoClient mongoClient;
	private final MongoCollection<Document> collection;
	private final RootReference<R> rootRef;
	private final BoskDriver<R> downstream;
	private final FlushLock flushLock;
	private final List<Reference<? extends EnumerableByIdentifier<?>>> separateCollections;
	private final BsonSurgeon bsonSurgeon;

	private volatile BsonInt64 revisionToSkip = null;

	static final BsonString ROOT_DOCUMENT_ID = new BsonString("|");

	PandoFormatDriver(
		Bosk<R> bosk,
		MongoCollection<Document> collection,
		MongoDriverSettings driverSettings,
		PandoFormat format, BsonPlugin bsonPlugin,
		MongoClient mongoClient,
		FlushLock flushLock,
		BoskDriver<R> downstream
	) {
		this.description = PandoFormatDriver.class.getSimpleName() + ": " + driverSettings;
		this.settings = driverSettings;
		this.format = format;
		this.mongoClient = mongoClient;
		this.formatter = new Formatter(bosk, bsonPlugin);
		this.collection = collection;
		this.rootRef = bosk.rootReference();
		this.downstream = downstream;
		this.flushLock = flushLock;

		separateCollections = format.separateCollections().stream()
			.map(s -> referenceTo(s, rootRef))
			.sorted(comparing((Reference<?> ref) -> ref.path().length()).reversed())
			.collect(toList());
		this.bsonSurgeon = new BsonSurgeon(separateCollections);
	}

	private static Reference<EnumerableByIdentifier<Entity>> referenceTo(String pathString, RootReference<?> rootRef) {
		try {
			return rootRef.then(enumerableByIdentifier(Entity.class), parseParameterized(pathString));
		} catch (InvalidTypeException e) {
			throw new IllegalArgumentException("Invalid configuration -- path does not point to a Catalog or SideTable: " + pathString, e);
		}
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		BsonValue value = formatter.object2bsonValue(newValue, target.targetType());
		try (Transaction txn = new Transaction()) {
			BsonDocument rootUpdate;
			if (value instanceof BsonDocument) {
				deleteParts(target);
				BsonDocument mainPart = upsertAndRemoveSubParts(target, value.asDocument());
				Reference<?> mainRef = mainRef(target);
				if (rootRef.equals(mainRef)) {
					rootUpdate = replacementDoc(target, value, rootRef);
				} else {
					if (target.equals(mainRef)) {
						// Upsert the main doc
					} else {
						// Update part of the main doc (which must already exist)
						String key = dottedFieldNameOf(target, mainRef);
						LOGGER.debug("| Set field {} in {}: {}", key, mainRef, value);
						BsonDocument mainUpdate = new BsonDocument("$set", new BsonDocument(key, value));
						BsonDocument filter = new BsonDocument("_id", mainPart.get("_id"));
						doUpdate(mainUpdate, standardPreconditions(target, mainRef, filter));
					}
					// On the root doc, we're only bumping the revision
					rootUpdate = blankUpdateDoc();
				}
			} else {
				rootUpdate = replacementDoc(target, value, rootRef);
			}
			doUpdate(rootUpdate, standardRootPreconditions(target));
			txn.commit();
		}
	}

	private Reference<?> mainRef(Reference<?> target) {
		// The main reference is the "deepest" one that matches the target reference.
		// separateCollections is in descending order of depth.
		// TODO: This could be done more efficiently, perhaps using a trie
		int targetPathLength = target.path().length();
		for (Reference<? extends EnumerableByIdentifier<?>> candidate: separateCollections) {
			if (candidate.path().length() <= targetPathLength) {
				if (candidate.path().matches(target.path().truncatedTo(candidate.path().length()))) {
					return candidate;
				}
			}
		}
		return rootRef;
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		BsonDocument filter = standardRootPreconditions(target);
		filter.put(dottedFieldNameOf(target, rootRef), new BsonDocument("$exists", FALSE));
		BsonValue value = formatter.object2bsonValue(newValue, target.targetType());
		try (Transaction txn = new Transaction()) {
			if (value instanceof BsonDocument) {
				deleteParts(target);
				BsonDocument mainPart = upsertAndRemoveSubParts(target, value.asDocument());
			}
			if (doUpdate(replacementDoc(target, value, rootRef), filter)) {
				LOGGER.debug("| Object initialized");
				txn.commit();
			} else {
				LOGGER.debug("| No update");
				txn.abort(); // Undo any changes to the part documents
			}
		}
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		try (Transaction txn = new Transaction()) {
			deleteParts(target);
			if (doUpdate(deletionDoc(target, rootRef), standardRootPreconditions(target))) {
				txn.commit();
			} else {
				txn.abort();
			}
		}
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		BsonValue value = formatter.object2bsonValue(newValue, target.targetType());
		try (Transaction txn = new Transaction()) {
			if (value instanceof BsonDocument) {
				deleteParts(target);
				BsonDocument mainPart = upsertAndRemoveSubParts(target, value.asDocument());
			}
			if (doUpdate(
				replacementDoc(target, value, rootRef),
				explicitPreconditions(target, precondition, requiredValue))
			) {
				txn.commit();
			} else {
				txn.abort();
			}
		}
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		try (Transaction txn = new Transaction()) {
			if (doUpdate(
				deletionDoc(target, rootRef),
				explicitPreconditions(target, precondition, requiredValue))
			) {
				txn.commit();
			} else {
				txn.abort();
			}
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

	@Override
	public StateAndMetadata<R> loadAllState() throws IOException, UninitializedCollectionException {
		List<Document> allParts = new ArrayList<>();
		try (MongoCursor<Document> cursor = collection
			.withReadConcern(LOCAL) // The revision field needs to be the latest
			.find(regex("_id", "^" + Pattern.quote("|")))
			.cursor()
		) {
			while (cursor.hasNext()) {
				allParts.add(cursor.next());
			}
		} catch (NoSuchElementException e) {
			throw new UninitializedCollectionException("No existing document", e);
		}
		Document mainPart = allParts.get(allParts.size()-1);
		Long revision = mainPart.get(DocumentFields.revision.name(), 0L);
		List<BsonDocument> partsList = allParts
			.stream()
			.map(d -> d.toBsonDocument(BsonDocument.class, formatter.codecRegistry()))
			.collect(toList());

		BsonDocument combinedState = bsonSurgeon.gather(partsList);
		R root = formatter.document2object(combinedState, rootRef);
		BsonInt64 rev = new BsonInt64(revision);
		return new StateAndMetadata<>(root, rev);
	}

	@Override
	public void initializeCollection(StateAndMetadata<R> priorContents) {
		BsonValue initialState = formatter.object2bsonValue(priorContents.state, rootRef.targetType());
		BsonInt64 newRevision = new BsonInt64(1 + priorContents.revision.longValue());

		try (Transaction txn = new Transaction()) {
			LOGGER.debug("** Initial tenant upsert for {}", ROOT_DOCUMENT_ID.getValue());
			if (initialState instanceof BsonDocument) {
				BsonDocument mainPart = upsertAndRemoveSubParts(rootRef, initialState.asDocument()); // Mutates initialState!
			}
			BsonDocument update = new BsonDocument("$set", initialDocument(initialState, newRevision));
			BsonDocument filter = rootDocumentFilter();
			UpdateOptions options = new UpdateOptions().upsert(true);
			LOGGER.trace("| Filter: {}", filter);
			LOGGER.trace("| Update: {}", update);
			LOGGER.trace("| Options: {}", options);
			UpdateResult result = collection.updateOne(filter, update, options);
			LOGGER.debug("| Result: {}", result);
			if (settings.experimental().manifestMode() == CREATE_IF_ABSENT) {
				writeManifest();
			}
			txn.commit();
		}
	}

	private void writeManifest() {
		BsonDocument doc = new BsonDocument("_id", MANIFEST_ID);
		doc.putAll((BsonDocument) formatter.object2bsonValue(Manifest.forPando(format), Manifest.class));
		BsonDocument update = new BsonDocument("$set", doc);
		BsonDocument filter = new BsonDocument("_id", MANIFEST_ID);
		UpdateOptions options = new UpdateOptions().upsert(true);
		LOGGER.debug("| Initial manifest: {}", doc);
		UpdateResult result = collection.updateOne(filter, update, options);
		LOGGER.debug("| Manifest result: {}", result);
	}

	/**
	 * We're required to cope with anything we might ourselves do in {@link #initializeCollection}.
	 */
	@Override
	public void onEvent(ChangeStreamDocument<Document> event) throws UnprocessableEventException {
		if (event.getDocumentKey() == null) {
			throw new UnprocessableEventException("Null document key", event.getOperationType());
		}
		if (MANIFEST_ID.equals(event.getDocumentKey().get("_id"))) {
			onManifestEvent(event);
			return;
		}
		if (!DOCUMENT_FILTER.equals(event.getDocumentKey())) {
			LOGGER.debug("Ignoring event for unrecognized document key: {}", event.getDocumentKey());
			return;
		}
		switch (event.getOperationType()) {
			case INSERT: case REPLACE: {
				// TODO: Handle insert/replace of part documents
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
				// TODO: Queue up part documents, and submit downstream only once the main event arrives
				LOGGER.debug("| Replace {}", rootRef);
				downstream.submitReplacement(rootRef, newRoot);
				flushLock.finishedRevision(revision);
			} break;
			case UPDATE: {
				// TODO: Handle update of part documents
				// TODO: Include any queued up part documents
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
				// TODO: Handle deletion of part documents
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
	 * but outside that, we want to be as strict as possible
	 * so incompatible database changes don't go unnoticed.
	 */
	private void onManifestEvent(ChangeStreamDocument<Document> event) throws UnprocessableEventException {
		if (event.getOperationType() == INSERT) {
			Document manifestDoc = requireNonNull(event.getFullDocument());
			Manifest manifest;
			try {
				manifest = formatter.decodeManifest(manifestDoc);
			} catch (UnrecognizedFormatException e) {
				throw new UnprocessableEventException("Invalid manifest", e, event.getOperationType());
			}
			if (!manifest.equals(Manifest.forPando(format))) {
				throw new UnprocessableEventException("Manifest indicates format has changed", event.getOperationType());
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

	private BsonInt64 getRevisionFromFullDocumentEvent(Document fullDocument) {
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

	private static BsonInt64 getRevisionFromUpdateEvent(ChangeStreamDocument<Document> event) {
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

	//
	// MongoDB helpers
	//

	/**
	 * @return Non-null revision number as per the database.
	 * If the database contains no revision number, returns {@link Formatter#REVISION_ZERO}.
	 */
	private BsonInt64 readRevisionNumber() throws FlushFailureException {
		LOGGER.debug("readRevisionNumber");
		try {
			try (MongoCursor<Document> cursor = collection
				.withReadConcern(LOCAL) // The revision field needs to be the latest
				.find(DOCUMENT_FILTER)
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

	private BsonDocument rootDocumentFilter() {
		return new BsonDocument("_id", ROOT_DOCUMENT_ID);
	}

	private <T> BsonDocument standardRootPreconditions(Reference<T> target) {
		return standardPreconditions(target, rootRef, rootDocumentFilter());
	}

	private <T> BsonDocument standardPreconditions(Reference<T> target, Reference<?> startingRef, BsonDocument filter) {
		if (!target.path().equals(startingRef.path())) {
			String enclosingObjectKey = dottedFieldNameOf(enclosingReference(target), startingRef);
			BsonDocument condition = new BsonDocument("$type", new BsonString("object"));
			filter.put(enclosingObjectKey, condition);
			LOGGER.debug("| Precondition: {} {}", enclosingObjectKey, condition);
		}
		return filter;
	}

	private <T> BsonDocument explicitPreconditions(Reference<T> target, Reference<Identifier> preconditionRef, Identifier requiredValue) {
		BsonDocument filter = standardRootPreconditions(target);
		BsonDocument precondition = new BsonDocument("$eq", new BsonString(requiredValue.toString()));
		filter.put(dottedFieldNameOf(preconditionRef, rootRef), precondition);
		return filter;
	}

	private <T> BsonDocument replacementDoc(Reference<T> target, BsonValue value, Reference<?> startingRef) {
		String key = dottedFieldNameOf(target, startingRef);
		LOGGER.debug("| Set field {}: {}", key, value);
		return blankUpdateDoc()
			.append("$set", new BsonDocument(key, value));
	}

	private <T> BsonDocument deletionDoc(Reference<T> target, Reference<?> startingRef) {
		String key = dottedFieldNameOf(target, startingRef);
		LOGGER.debug("| Unset field {}", key);
		return blankUpdateDoc().append("$unset", new BsonDocument(key, new BsonNull())); // Value is ignored
	}

	private BsonDocument blankUpdateDoc() {
		return new BsonDocument("$inc", new BsonDocument(DocumentFields.revision.name(), new BsonInt64(1)));
	}

	private BsonDocument initialDocument(BsonValue initialState, BsonInt64 revision) {
		BsonDocument fieldValues = new BsonDocument("_id", ROOT_DOCUMENT_ID);

		fieldValues.put(DocumentFields.path.name(), new BsonString("/"));
		fieldValues.put(DocumentFields.state.name(), initialState);
		fieldValues.put(DocumentFields.revision.name(), revision);

		return fieldValues;
	}

	/**
	 * @return true if something changed
	 */
	private boolean doUpdate(BsonDocument updateDoc, BsonDocument filter) {
		LOGGER.debug("| Update: {}", updateDoc);
		LOGGER.debug("| Filter: {}", filter);
		if (settings.testing().eventDelayMS() < 0) {
			LOGGER.debug("| Sleeping");
			try {
				Thread.sleep(-settings.testing().eventDelayMS());
			} catch (InterruptedException e) {
				LOGGER.debug("| Interrupted");
			}
		}
		UpdateResult result = collection.updateOne(filter, updateDoc, new UpdateOptions().upsert(true));
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
	private void replaceUpdatedFields(@Nullable BsonDocument updatedFields) {
		if (updatedFields != null) {
			for (Map.Entry<String, BsonValue> entry : updatedFields.entrySet()) {
				String dottedName = entry.getKey();
				if (dottedName.startsWith(DocumentFields.state.name())) {
					Reference<Object> ref;
					try {
						ref = Formatter.referenceTo(dottedName, rootRef);
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
	private void deleteRemovedFields(@Nullable List<String> removedFields, OperationType operationType) throws UnprocessableEventException {
		if (removedFields != null) {
			for (String dottedName : removedFields) {
				if (dottedName.startsWith(DocumentFields.state.name())) {
					Reference<Object> ref;
					try {
						ref = Formatter.referenceTo(dottedName, rootRef);
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

	private <T> void deleteParts(Reference<T> target) {
		String prefix;
		if (target.path().isEmpty()) {
			prefix = "|";
		} else {
			prefix = "|" + String.join("|", containerSegments(rootRef, target)) + "|";
		}

		// Every doc whose ID starts with the prefix and has at least one more character
		Bson filter = regex("_id", "^" + Pattern.quote(prefix) + ".");

		DeleteResult result = collection.deleteMany(filter);
		LOGGER.debug("deleteParts({}) result: {} filter: {}", target, result, filter);
	}

	/**
	 * @param value is mutated to stub-out the parts written to the database
	 * @return the <em>main part</em> document, representing the root of the tree of part-documents
	 * (which is not the root of the bosk state tree, unless of course <code>target</code> is the root reference)
	 */
	private <T> BsonDocument upsertAndRemoveSubParts(Reference<T> target, BsonDocument value) {
		List<BsonDocument> allParts = bsonSurgeon.scatter(rootRef, target, value);
		// NOTE: `value` has now been mutated so the parts have been stubbed out

		List<BsonDocument> subParts = allParts.subList(0, allParts.size() - 1);

		UpdateOptions options = new UpdateOptions().upsert(true);
		for (BsonDocument part: subParts) {
			BsonDocument update = new BsonDocument("$set", part);
			BsonDocument filter = new BsonDocument("_id", part.get("_id"));
			collection.updateOne(filter, update, options);
		}

		return allParts.get(allParts.size()-1);
	}

	private class Transaction implements AutoCloseable {
		private final ClientSession session;

		Transaction() {
			ClientSessionOptions sessionOptions = ClientSessionOptions.builder()
				.causallyConsistent(true)
				.defaultTransactionOptions(TransactionOptions.builder()
					.writeConcern(WriteConcern.MAJORITY)
					.readConcern(ReadConcern.MAJORITY)
					.build())
				.build();
			session = mongoClient.startSession(sessionOptions);
			session.startTransaction();
		}

		public void commit() {
			session.commitTransaction();
		}

		/**
		 * Not strictly necessary, because this is the default if the transaction
		 * is not committed; however, it makes the calling code more self-documenting.
		 */
		public void abort() {
			session.abortTransaction();
		}

		@Override
		public void close() {
			session.close();
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
	private static final BsonDocument DOCUMENT_FILTER = new BsonDocument("_id", ROOT_DOCUMENT_ID);
	private static final Logger LOGGER = LoggerFactory.getLogger(PandoFormatDriver.class);
}
