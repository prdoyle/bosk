package works.bosk;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static works.bosk.OverloadController.Mode.CATCH_UP;
import static works.bosk.OverloadController.Mode.OVERLOAD;
import static works.bosk.OverloadController.Mode.PROBATION;
import static works.bosk.OverloadController.Mode.STANDARD;

class OverloadControllerTest {
	final OverloadController controller = new OverloadController(10);

	@Test
	void startsInStandardModeWithOneQueueLengthOfProbeBudget() {
		assertEquals(STANDARD, controller.mode(), "A new controller should start in standard mode");
		assertEquals(10, controller.probeBudget(), "The initial probe budget should be one queue length");
	}

	@Test
	void probeBudgetIsProportionalToTheQueueLimit() {
		assertEquals(1000, new OverloadController(1000).probeBudget(), "The probe budget should be one limit's worth of hook work");
	}

	@Test
	void probationRunsForTheProbeBudget() {
		startOverload();
		controller.hookWorkDrained();
		assertEquals(PROBATION, controller.mode(), "Starting a probation should enter probation mode");
		assertEquals(10, controller.probeRemaining(), "The probation should have one queue length of executions");
	}

	@Test
	void probationSucceedsAfterItsBudget() {
		startOverload();
		controller.hookWorkDrained();
		for (int i = 0; i < 9; i++) {
			controller.recordWork(1);
			assertEquals(PROBATION, controller.mode(), "The probation should not finish early");
		}
		controller.recordWork(1);
		assertEquals(CATCH_UP, controller.mode(), "A successful probation should start the final catch-up");
	}

	@Test
	void queuedWorkReachesTheThreshold() {
		controller.recordQueuedWork(9);
		assertEquals(STANDARD, controller.mode(), "Nine queued executions should be below the threshold");
		controller.recordQueuedWork(1);
		assertEquals(OVERLOAD, controller.mode(), "Reaching the threshold should enter overload mode");
	}

	@Test
	void dequeuedWorkReducesTheCount() {
		controller.recordQueuedWork(9);
		controller.recordDequeuedWork(2);
		controller.recordQueuedWork(2);
		assertEquals(STANDARD, controller.mode(), "Dequeued work should no longer count toward the threshold");
	}

	@Test
	void clearQueuedWorkForgetsTheCount() {
		controller.recordQueuedWork(9);
		controller.clearQueuedWork();
		controller.recordQueuedWork(9);
		assertEquals(STANDARD, controller.mode(), "Cleared work should not count toward the threshold");
	}

	@Test
	void zeroThresholdStartsInOverloadMode() {
		assertEquals(OVERLOAD, new OverloadController(0).mode(), "A zero threshold should start in overload mode");
		assertEquals(STANDARD, new OverloadController(1).mode(), "A threshold of one should start in standard mode");
	}

	@Test
	void zeroThresholdStaysInOverloadModeAfterReset() {
		OverloadController alwaysOverload = new OverloadController(0);
		alwaysOverload.reset();
		assertEquals(OVERLOAD, alwaysOverload.mode(), "A zero threshold should not reset to standard mode");
	}

	@Test
	void onlyZeroIsPermanentOverload() {
		assertTrue(new OverloadController(0).isPermanentOverload(), "Only a zero threshold should stay in overload mode");
		assertFalse(new OverloadController(1).isPermanentOverload(), "A threshold of one should use the ordinary path");
		assertFalse(new OverloadController(2).isPermanentOverload(), "A threshold of two should allow standard delivery");
	}

	@Test
	void resetForgetsQueuedWork() {
		startOverload();
		controller.reset();
		controller.recordQueuedWork(9);
		assertEquals(STANDARD, controller.mode(), "Reset should forget queued work");
	}

	@Test
	void probationFailureReturnsToOverload() {
		startOverload();
		controller.hookWorkDrained();
		controller.recordQueuedWork(10);
		assertEquals(OVERLOAD, controller.mode(), "A probation overflow should return to overload mode");
	}

	@Test
	void catchUpFinishesInStandardMode() {
		completeSuccessfulProbation();
		assertEquals(STANDARD, controller.mode(), "A completed catch-up should enter standard mode");
	}

	@Test
	void earlyStandardOverflowDoublesTheProbeBudget() {
		completeSuccessfulProbation();
		assertEquals(10, controller.probeBudget(), "The base probe budget should be unchanged");
		controller.recordQueuedWork(10);
		assertEquals(20, controller.probeBudget(), "An early standard overflow should double the probe budget");
	}

	@Test
	void repeatedEarlyOverflowsKeepDoublingTheProbeBudget() {
		completeSuccessfulProbation();
		controller.recordQueuedWork(10);
		completeSuccessfulProbation();
		controller.recordQueuedWork(10);
		completeSuccessfulProbation();
		controller.recordQueuedWork(10);
		assertEquals(80, controller.probeBudget(), "The probe budget should keep doubling without a cap");
	}

	@Test
	void lateStandardOverflowDoesNotGrowTheProbeBudget() {
		completeSuccessfulProbation();
		for (int i = 0; i < 10; i++) {
			controller.recordWork(1);
		}
		controller.recordQueuedWork(10);
		assertEquals(10, controller.probeBudget(), "A validated probation should not be extended by a later overflow");
	}

	@Test
	void probeBudgetDecaysAfterSustainedStandardWork() {
		completeSuccessfulProbation();
		controller.recordQueuedWork(10);
		assertEquals(20, controller.probeBudget(), "An early overflow should double the probe budget");

		completeSuccessfulProbation();
		for (int i = 0; i < 20; i++) {
			controller.recordWork(1);
		}
		assertEquals(10, controller.probeBudget(), "Sustained standard work should decay the multiplier linearly");
	}

	/**
	 * Queues a threshold's worth of work, which the drain would then remove before
	 * the backlog empties, leaving the controller in overload mode.
	 */
	private void startOverload() {
		controller.recordQueuedWork(10);
		controller.clearQueuedWork();
	}

	private void completeSuccessfulProbation() {
		if (controller.mode() == STANDARD) {
			controller.recordQueuedWork(10);
		}
		// The queue drains into the first overload batch before the backlog empties.
		controller.clearQueuedWork();
		long budget = controller.probeBudget();
		controller.hookWorkDrained();
		for (long i = 1; i < budget; i++) {
			controller.recordWork(1);
			assertEquals(PROBATION, controller.mode(), "The probation should continue");
		}
		controller.recordWork(1);
		controller.catchUpFinished();
		assertEquals(STANDARD, controller.mode(), "The catch-up should enter standard mode");
	}
}
