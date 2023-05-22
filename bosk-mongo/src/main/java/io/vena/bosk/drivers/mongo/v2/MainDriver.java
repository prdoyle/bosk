package io.vena.bosk.drivers.mongo.v2;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.Entity;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.drivers.mongo.MongoDriver;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.RequiredArgsConstructor;
import org.bson.BsonDocument;
import org.bson.Document;

import static java.lang.Thread.State.NEW;
import static lombok.AccessLevel.PACKAGE;

@RequiredArgsConstructor(access = PACKAGE)
public class MainDriver<R extends Entity> implements MongoDriver<R> {
	private final MongoCollection<Document> collection;
	private final MongoClient client;
	private final ChangeEventReceiver receiver;
	private final Lock lock = new ReentrantLock();
	private final BoskDriver<R> downstream;
	private final Reference<R> rootRef;

	private volatile FormatDriver<R> formatDriver;
	private volatile BsonDocument lastProcessedResumeToken;
	private volatile Thread eventProcessingThread;

	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		R result = resynchronizeEventThread();
		if (eventProcessingThread.getState() == NEW) {
			eventProcessingThread.start();
		}
		if (result == null) {
			return downstream.initialRoot(rootType);
		} else {
			return result;
		}
	}

	private void panic() throws InterruptedException {
		R result = resynchronizeEventThread();
		if (result != null) {
			downstream.submitReplacement(rootRef, result);
		}
		if (eventProcessingThread.getState() == NEW) {
			eventProcessingThread.start();
		}
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

	private R resynchronizeEventThread() throws InterruptedException {
		receiver.initialize(null); // TODO
		if (lock.tryLock()) {
			// Shut down existing event thread
			eventProcessingThread.interrupt();
			eventProcessingThread.join(10_000);  // TODO: config

			// Open cursor and orient if necessary
			MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor;
			ChangeStreamDocument<Document> event;
			if (lastProcessedResumeToken == null) {
				ChangeStreamIterable<Document> iterable = collection.watch();
				cursor = iterable.cursor();
				event = cursor.tryNext();
				if (event == null) {
					// In this case, tryNext() has caused the cursor to point to
					// a token in the past, so we can reliably use that.
					lastProcessedResumeToken = cursor.getResumeToken();
				}
			} else {
				ChangeStreamIterable<Document> iterable = collection.watch().resumeAfter(lastProcessedResumeToken);
				cursor = iterable.cursor();
				event = null;
			}

			// Determine format and instantiate format driver
			FormatDriver<R> formatDriver = null; // TODO
			StateResult<R> result = formatDriver.loadAllState();

			// Kick off new event processing thread; caller will run it
			eventProcessingThread = new Thread(()->{/* TODO */}, "MongoDriver event processor");

			return result.state;
		} else {
			// Another thread is already reinitializing. Nothing to do.
			return null;
		}
		/*
		State distinctions:
		- "Oriented" vs "Disoriented" based on whether we've ever seen a resume token
		- "Initialized" vs "Uninitialized" based on whether the bosk _has_ a state
			- We should call downstream.initialState() at most once

		1. Open change stream cursor
			- If disoriented, call tryNext and get the resume token; also save the returned event for step 4
		2. Determine format and instantiate format driver
		3. Use it to load initial state and revision
			- At this point, we're Initialized
		4. Initiate event-processing thread using cursor, revision, and possibly one event
			- But paused, so it's not sending anything downstream yet!
		5. Return the initial state

		Caller is expected to:
		- Do the right thing with the returned state (return from initialState or submit downstream)
		  - On successful downstream submission, change to the "initialized" state
		- Resume the event-processing thread

		Special cases:
		- During 1, if the collection doesn't exist, create it and initialize from downstream.initialState()
		- If any MongoException occurs:
		 	- If we're uninitialized, then call downstream.initialState() and return that
		 	- Else return null, indicating that the existing bosk state should be retained
		*/
		return null;
	}
}
