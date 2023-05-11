package io.vena.bosk.drivers.mongo.modal;

import com.mongodb.client.MongoCursor;
import io.vena.bosk.Entity;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.drivers.mongo.MongoDriver;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.NoSuchElementException;
import lombok.RequiredArgsConstructor;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.vena.bosk.drivers.mongo.Formatter.DocumentFields.state;

@RequiredArgsConstructor
public class SingleDocFormatDriver<R extends Entity> implements MongoDriver<R> {
	final BoskCollection collection;

	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		LOGGER.debug("+ initialRoot");

		try (MongoCursor<Document> cursor = collection.find(documentFilter()).limit(1).cursor()) {
			Document newDocument = cursor.next();
			Document newState = newDocument.get(state.name(), Document.class);
			if (newState == null) {
				LOGGER.debug("| No existing state; delegating downstream");
			} else {
				LOGGER.debug("| From database: {}", newState);
				bumpRevision();
				return formatter.document2object(newState, rootRef);
			}
		} catch (NoSuchElementException e) {
			LOGGER.debug("| No tenant document; delegating downstream");
		}

		R root = receiver.initialRoot(rootType);
		ensureDocumentExists(formatter.object2bsonValue(root, rootType), "$setOnInsert");
		bumpRevision();
		return root;
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

	private static final Logger LOGGER = LoggerFactory.getLogger(SingleDocFormatDriver.class);
}
