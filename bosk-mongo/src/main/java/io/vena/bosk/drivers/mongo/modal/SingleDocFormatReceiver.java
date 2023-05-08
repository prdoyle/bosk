package io.vena.bosk.drivers.mongo.modal;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.mongodb.lang.Nullable;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.Entity;
import io.vena.bosk.Reference;
import io.vena.bosk.drivers.mongo.Formatter;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.vena.bosk.drivers.mongo.Formatter.DocumentFields.revision;
import static io.vena.bosk.drivers.mongo.Formatter.referenceTo;
import static java.util.Collections.newSetFromMap;

@RequiredArgsConstructor
public class SingleDocFormatReceiver<R extends Entity> implements EventReceiver {
	private final Reference<R> rootRef;
	private final Formatter formatter;
	private final Runnable reconnectAction;
	private final Consumer<Long> revisionListener;
	private final BoskDriver<R> downstream;

	@Override
	public void onUpdate(ChangeStreamDocument<Document> event) {
		UpdateDescription updateDescription = event.getUpdateDescription();
		if (updateDescription != null) {
			replaceUpdatedFields(updateDescription.getUpdatedFields());
			deleteRemovedFields(updateDescription.getRemovedFields());

			// Now that we've done everything else, we can report that we've processed the event
			reportRevision(updateDescription.getUpdatedFields());
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
				if (dottedName.startsWith(Formatter.DocumentFields.state.name())) {
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
	private void deleteRemovedFields(@Nullable List<String> removedFields) {
		if (removedFields != null) {
			for (String dottedName : removedFields) {
				if (dottedName.startsWith(Formatter.DocumentFields.state.name())) {
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

	/**
	 * Report the new revision to {@link #revisionListener}
	 */
	private void reportRevision(@Nullable BsonDocument updatedFields) {
		if (updatedFields != null) {
			BsonInt64 newValue = updatedFields.getInt64(revision.name(), null);
			if (newValue == null) {
				LOGGER.error("No revision field");
				reconnectAction.run();
			} else {
				LOGGER.debug("Revision {}", newValue);
				revisionListener.accept(newValue.longValue());
			}
		}
	}

	@Override
	public void onUpsert(ChangeStreamDocument<Document> event) {
		reconnectAction.run();
	}

	@Override
	public void onUnrecognizedEvent(ChangeStreamDocument<Document> event) {
		reconnectAction.run();
	}

	@Override
	public void onException(Exception e) {
		reconnectAction.run();
	}

	private static final Set<String> ALREADY_WARNED = newSetFromMap(new ConcurrentHashMap<>());
	private static final Logger LOGGER = LoggerFactory.getLogger(SingleDocFormatReceiver.class);
}
