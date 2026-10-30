package works.bosk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static works.bosk.OverloadController.Mode.OVERLOAD;
import static works.bosk.OverloadController.Mode.STANDARD;

/**
 * Decides which delivery mode the hook runner is in.
 *
 * <p>
 * The hook runner reports the events that drive this state machine: enqueued and
 * dequeued hook executions. This class turns those events into mode transitions;
 * it knows nothing about the queue's contents, triggers, or hook calls.
 *
 * <p>
 * Once the queued work reaches the threshold, delivery switches to overload mode.
 */
final class OverloadController {
	enum Mode {
		/**
		 * Standard delivery: each reference changed by an update is executed
		 * separately, with the state snapshot taken immediately after that update.
		 */
		STANDARD,
		/**
		 * Compacted delivery: each batch contains at most one execution per hook
		 * registration and reference, executed against a later state snapshot.
		 */
		OVERLOAD;

		/**
		 * @return whether this mode records each update's notifications as pending
		 * calls rather than queueing them
		 */
		boolean isCompacting() {
			return switch (this) {
				case STANDARD -> false;
				case OVERLOAD -> true;
			};
		}
	}

	private final int overloadThreshold;
	/**
	 * Hook executions represented by the runner's queue: one per hook registration
	 * matching each queued update. The queue itself stays with the runner; this total
	 * lives here so that the threshold decision does too. Guarded by this object's
	 * monitor.
	 */
	private int queuedHookCount;

	/**
	 * Volatile because the draining thread reads the mode outside the driver's monitor.
	 */
	private volatile Mode mode = STANDARD;

	OverloadController(int overloadThreshold) {
		this.overloadThreshold = overloadThreshold;
		LOGGER.debug("Starting in {} mode with an overload threshold of {}", mode, overloadThreshold);
	}

	Mode mode() {
		return mode;
	}

	/**
	 * Records hook executions added to the queue, entering overload mode if they
	 * reach the threshold.
	 */
	synchronized void recordQueuedWork(int units) {
		assert !mode.isCompacting() : "Only standard-style delivery queues work";
		queuedHookCount += units;
		if (queuedHookCount >= overloadThreshold) {
			LOGGER.warn("Hook backlog has reached the threshold ({}); entering overload mode", overloadThreshold);
			mode = OVERLOAD;
		}
	}

	/**
	 * Records hook executions removed from the queue.
	 */
	synchronized void recordDequeuedWork(int units) {
		queuedHookCount -= units;
	}

	/**
	 * Forgets the queued hook executions, as when the queue is drained or discarded.
	 */
	synchronized void clearQueuedWork() {
		queuedHookCount = 0;
	}

	/**
	 * Returns to standard mode after an abnormal stop.
	 */
	synchronized void reset() {
		mode = STANDARD;
		queuedHookCount = 0;
		LOGGER.debug("Hook work abandoned; mode is now {}", mode);
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(Bosk.HOOK_LOGGER_NAME);
}
