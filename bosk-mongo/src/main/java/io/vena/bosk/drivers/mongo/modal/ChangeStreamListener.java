package io.vena.bosk.drivers.mongo.modal;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.bson.Document;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * A mutable container for a pair of ({@link EventCursor}, {@link EventReceiver}).
 * Feeds the events from the cursor to the receiver.
 * On any sort of error, will call a supplied <code>reconnectAction</code>
 * (which may call {@link #restart} to kick off the listener again.
 */
public class ChangeStreamListener {
	private final Runnable reconnect;
	private volatile State currentState;
	private Deque<Future<?>> runningTasks = new ArrayDeque<>();

	private final Semaphore lock = new Semaphore(1);
	private final ExecutorService ex = Executors.newFixedThreadPool(1);

	public ChangeStreamListener(Runnable reconnectAction) {
		this.reconnect = reconnectAction;
	}

	@RequiredArgsConstructor
	private static final class State {
		final EventCursor eventCursor;
		final EventReceiver receiver;
	}

	public void stop() {
		changeState(()->null);
	}

	public void restart(EventCursor eventCursor, EventReceiver receiver) {
		changeState(() -> {
			State state =  new State(eventCursor, receiver);
			runningTasks.add(ex.submit(() -> eventLoop(state)));
			return state;
		});
	}

	private void eventLoop(State state) {
		try {
			ChangeStreamDocument<Document> event = state.eventCursor.next();
		} finally {
			reconnect.run();
		}
	}

	private void changeState(Supplier<State> newState) {
		try {
			if (lock.tryAcquire(10, MINUTES)) { // TODO: Configurable
				// Shut down all running tasks
				Future<?> task;
				while ((task = runningTasks.poll()) != null) {
					task.cancel(true);
					task.get(10, MINUTES);
				}
				// Set the new state. This can also kick off a new running task
				currentState = newState.get();
			} else {
				throw new TimeoutException("Timed out waiting for lock");
			}
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			throw new ReconnectionException(e);
		}
	}
}
