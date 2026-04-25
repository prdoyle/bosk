package works.bosk.drivers.mongo.internal;

import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import java.io.IOException;
import org.bson.BsonDocument;
import works.bosk.BoskContext;
import works.bosk.StateTreeNode;
import works.bosk.drivers.mongo.MongoDriver;

/**
 * Additional {@link MongoDriver} functionality that the format-specific drivers must implement.
 * <p>
 * Implementations of this are responsible for the following:
 * <ol><li>
 *     Serializing and deserializing the database contents
 * </li><li>
 *     Processing change stream events via {@link #onEvent}
 * </li><li>
 *     Implementing {@link #flush()} (consider using {@link FlushLock})
 * </li><li>
 *     Propagating {@link BoskContext context} downstream.
 * </li></ol>
 *
 * Implementations are not responsible for:
 * <ol><li>
 *     Handling exceptions from {@link MongoClient} or {@link MongoChangeStreamCursor}
 * </li><li>
 *     Determining the database format
 * </li><li>
 *     Implementing {@link #initialState} or {@link #refurbish()}
 * </li></ol>
 */
sealed public interface FormatDriver<R extends StateTreeNode>
	extends MongoDriver
	permits AbstractFormatDriver, DisconnectedDriver {
	void onEvent(ChangeStreamDocument<BsonDocument> event) throws UnprocessableEventException;

	/**
	 * Loads the entire collection contents for the purpose of establishing the bosk state tree.
	 * This involves more than merely reading the state:
	 * this also has the side effect of establishing the state that the
	 * driver "knows about".
	 * Specifically, it ensures that change stream events before this point are ignored,
	 * and that {@link #flush()} operations won't wait if the state hasn't changed after this point.
	 *
	 * @throws UninitializedCollectionException if it looks like the database has not yet
	 * been created (as opposed to being in a damaged or unrecognizable state).
	 * This signals to {@link MainDriver} that it may, if appropriate,
	 * automatically initialize the collection.
	 */
	StateAndMetadata<R> loadAllState() throws IOException, UninitializedCollectionException;

	/**
	 * Initializes the collection to the given state.
	 * <p>
	 * Like {@link #loadAllState}, this also has the side effect of establishing
	 * the state that the driver "knows about".
	 * <p>
	 * Can assume that the collection is empty or nonexistent,
	 * in the sense that there is no mess to clean up,
	 * but should tolerate documents already existing,
	 * by using upsert or replace operations, for example.
	 * @param priorContents the desired state, with metadata representing a (possibly hypothetical)
	 * "prior" state of the database; in particular, the revision number should be incremented
	 * so that a {@link #flush} after a {@link #refurbish} succeeds in waiting for the new state.
	 */
	void initializeCollection(StateAndMetadata<R> priorContents);

	@Override
	default <RR extends StateTreeNode> InitialState<RR> initialState(Class<RR> rootType) {
		throw new UnsupportedOperationException(
			"FormatDriver doesn't need to implement initialState: MainDriver derives it from loadAllState");
	}

	@Override
	default void refurbish() {
		throw new UnsupportedOperationException(
			"FormatDriver doesn't need to implement refurbish: MainDriver derives it from loadAllState and initializeCollection");
	}
}
