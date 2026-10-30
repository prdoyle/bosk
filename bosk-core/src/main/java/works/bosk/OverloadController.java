package works.bosk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static works.bosk.OverloadController.Mode.CATCH_UP;
import static works.bosk.OverloadController.Mode.OVERLOAD;
import static works.bosk.OverloadController.Mode.PROBATION;
import static works.bosk.OverloadController.Mode.STANDARD;

/**
 * Decides which delivery mode the hook runner is in, and how much work to allow
 * before leaving overload mode.
 *
 * <p>
 * The hook runner reports the events that drive this state machine: enqueued and
 * dequeued hook executions, completed standard or probation work, and an empty
 * backlog. This class turns those events into mode transitions and probation
 * budgets; it knows nothing about the queue's contents, triggers, or hook calls.
 *
 * <p>
 * Overload mode is left through a bounded standard-mode probation. The probation
 * starts when the compacted backlog has drained, and runs subsequent updates
 * one at a time. It succeeds if it spends its whole budget of hook work without
 * overflowing the queue. A successful probation enters {@link Mode#CATCH_UP}, a
 * transient mode that runs one final compacted generation before the driver
 * returns to standard mode.
 *
 * <p>
 * A standard-mode overflow soon after a successful probation means the probation
 * was too short. The next probation uses twice as much hook work as its budget.
 * After sustained standard-mode work, the multiplier decays linearly back to one.
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
		OVERLOAD,
		/**
		 * An attempt at standard delivery, to see whether the driver can keep up
		 * and whether the overload has calmed down.
		 */
		PROBATION,
		/**
		 * A final compacted generation that clears the backlog accumulated during
		 * the probation, so hook latency returns to normal promptly.
		 */
		CATCH_UP;

		/**
		 * @return whether this mode records each update's notifications as pending
		 * calls rather than queueing them
		 */
		boolean isCompacting() {
			return switch (this) {
				case STANDARD, PROBATION -> false;
				case OVERLOAD, CATCH_UP -> true;
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
	private volatile Mode mode;
	/**
	 * The probe budget is this value times {@link #overloadThreshold}. Each doubling
	 * requires a successful probation that already executed the previous budget,
	 * so overflow is unreachable in practice. The first multiplication to overflow
	 * would be {@link #probeBudget()} when this multiplier reaches
	 * {@code Long.MAX_VALUE / overloadThreshold}; {@link Math#multiplyExact(long, long)}
	 * makes that assumption explicit.
	 */
	private long probeMultiplier = 1;
	private long probeRemaining;
	private boolean postProbationOverflowWindow;
	private int standardExecutions;
	private int decayExecutions;

	OverloadController(int overloadThreshold) {
		this.overloadThreshold = overloadThreshold;
		this.mode = isPermanentOverload() ? OVERLOAD : STANDARD;
		LOGGER.debug("Starting in {} mode with an overload threshold of {}", mode, overloadThreshold);
	}

	Mode mode() {
		return mode;
	}

	/**
	 * @return whether the driver always stays in overload mode: it starts there,
	 * never uses the queue, and never probes
	 */
	boolean isPermanentOverload() {
		return overloadThreshold == 0;
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
			if (mode == STANDARD) {
				enterOverload();
			} else {
				probationFailed();
			}
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

	synchronized long probeBudget() {
		return Math.multiplyExact(probeMultiplier, (long) overloadThreshold);
	}

	synchronized long probeRemaining() {
		return probeRemaining;
	}

	/**
	 * Enters overload mode after a standard-mode backlog overflow.
	 */
	private void enterOverload() {
		assert mode == STANDARD : "Overload mode is entered from standard mode";
		if (postProbationOverflowWindow) {
			probeMultiplier = Math.multiplyExact(probeMultiplier, 2);
			LOGGER.debug("Probation was too short; doubling the probe budget to {} hook executions", probeBudget());
		}
		mode = OVERLOAD;
		postProbationOverflowWindow = false;
		standardExecutions = 0;
		decayExecutions = 0;
		probeRemaining = 0;
		LOGGER.debug("Entering overload mode");
	}

	/**
	 * Tries standard delivery again after the hook work has drained, unless the
	 * driver is not in overload mode or the threshold opts out of standard delivery.
	 */
	synchronized void hookWorkDrained() {
		if (mode == OVERLOAD && !isPermanentOverload()) {
			mode = PROBATION;
			probeRemaining = probeBudget();
			LOGGER.debug("Starting a probation with {} hook executions of budget", probeRemaining);
		}
	}

	/**
	 * Records completed hook work in whichever mode the driver is in.
	 *
	 * @param units the number of hook registrations that matched the update
	 */
	synchronized void recordWork(int units) {
		switch (mode) {
			case STANDARD -> recordStandardWork(units);
			case PROBATION -> recordProbationWork(units);
			case OVERLOAD, CATCH_UP ->
				// The hook's own update changed the mode while the hook ran, so this
				// work does not belong to the current mode's budget.
				LOGGER.debug("Ignoring completed work in {} mode", mode);
		}
	}

	/**
	 * Records completed standard-mode hook work. The first threshold's worth of
	 * work validates a preceding probation; after that, each threshold's worth
	 * slowly reduces an inflated probe multiplier.
	 */
	private void recordStandardWork(int units) {
		if (postProbationOverflowWindow) {
			standardExecutions += units;
			if (standardExecutions >= overloadThreshold) {
				postProbationOverflowWindow = false;
				standardExecutions = 0;
				decayExecutions = 0;
				LOGGER.debug("Probation validated by sustained standard delivery");
			}
		} else {
			decayExecutions += units;
			if (decayExecutions >= overloadThreshold) {
				decayExecutions = 0;
				if (probeMultiplier > 1) {
					probeMultiplier--;
					LOGGER.debug("Decaying the probe budget to {} hook executions", probeBudget());
				}
			}
		}
	}

	private void recordProbationWork(int units) {
		assert mode == PROBATION : "Probation work only occurs in probation mode";
		probeRemaining -= units;
		LOGGER.trace("Probation work: {} executions; {} of the budget remains", units, probeRemaining);
		if (probeRemaining <= 0) {
			mode = CATCH_UP;
			LOGGER.debug("Probation spent its budget; entering catch-up");
		}
	}

	/**
	 * Returns to standard mode after an abnormal stop, keeping the probe multiplier.
	 */
	synchronized void reset() {
		mode = isPermanentOverload() ? OVERLOAD : STANDARD;
		queuedHookCount = 0;
		probeRemaining = 0;
		postProbationOverflowWindow = false;
		standardExecutions = 0;
		decayExecutions = 0;
		LOGGER.debug("Hook work abandoned; mode is now {}", mode);
	}

	/**
	 * Abandons a probation because the queue reached its limit.
	 */
	private void probationFailed() {
		assert mode == PROBATION : "A probation can only fail while in probation mode";
		mode = OVERLOAD;
		probeRemaining = 0;
		LOGGER.debug("Probation failed; returning to overload mode");
	}

	/**
	 * Completes the final catch-up generation and returns to standard mode.
	 */
	synchronized void catchUpFinished() {
		assert mode == CATCH_UP : "Catch-up completion requires catch-up mode";
		mode = STANDARD;
		postProbationOverflowWindow = true;
		standardExecutions = 0;
		decayExecutions = 0;
		LOGGER.debug("Catch-up finished; entering standard mode");
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(Bosk.HOOK_LOGGER_NAME);
}
