package io.vena.bosk.drivers.mongo.v2;

import com.mongodb.client.MongoClient;
import io.vena.bosk.Entity;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.drivers.mongo.MongoDriver;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.io.IOException;
import java.lang.reflect.Type;
import lombok.RequiredArgsConstructor;
import org.bson.BsonDocument;

import static lombok.AccessLevel.PACKAGE;

@RequiredArgsConstructor(access = PACKAGE)
public class MainDriver<R extends Entity> implements MongoDriver<R> {
	private final MongoClient client;
	private final ChangeEventReceiver listener;

	private volatile FormatDriver<R> formatDriver;

	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		return null;
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

	private BsonDocument whatsTheDealYo() {
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

		Caller is expected to do the right thing with the returned state (return from initialState
		or submit downstream) and to resume the event-processing thread.

		Special cases:
		- During 1, if the collection doesn't exist, create it and initialize from downstream.initialState()
		- If any MongoException occurs:
		 	- If we're uninitialized, then call downstream.initialState() and return that
		 	- Else return null, indicating that the existing bosk state should be retained
		*/
		/*
		1. Attempt to get a resume token
		- Check of DB. If not, create it
		- If collection doesn't exist, create it?
		- Open change stream cursor on collection
		- Load state from DB including revision number
		- "Register" the state:
			- If doing initialState, arrange to return the state
			- Else submit it downstream
		- Call cursor.tryNext() once to get a resume token
			- If this returns an event, we must arrange for the event-processing thread to process it
		- Initiate an event-processing thread, with the newly opened cursor,
		  configured to skip all events with revision number <= the one loaded
		2. If any part of #1 throws a MongoException
			- If doing initialState, call downstream.initialState() and arrange to return that
			- Else we already have a state. Do nothing.
			- In either case, if we haven't yet seen a resume token, WHAT
		 */
		return null;
	}
}
