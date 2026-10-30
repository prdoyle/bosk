package works.bosk;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static works.bosk.OverloadController.Mode.OVERLOAD;
import static works.bosk.OverloadController.Mode.STANDARD;

class OverloadControllerTest {
	final OverloadController controller = new OverloadController(10);

	@Test
	void startsInStandardMode() {
		assertEquals(STANDARD, controller.mode(), "A new controller should start in standard mode");
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
	void zeroThresholdIsReachedByItsFirstQueuedExecution() {
		OverloadController zeroThreshold = new OverloadController(0);
		zeroThreshold.recordQueuedWork(1);
		assertEquals(OVERLOAD, zeroThreshold.mode(), "A zero threshold should enter overload mode on its first queued execution");
	}

	@Test
	void resetReturnsToStandardMode() {
		controller.recordQueuedWork(10);
		controller.reset();
		assertEquals(STANDARD, controller.mode(), "Reset should return to standard mode");
	}

	@Test
	void resetForgetsQueuedWork() {
		controller.recordQueuedWork(10);
		controller.reset();
		controller.recordQueuedWork(9);
		assertEquals(STANDARD, controller.mode(), "Reset should forget queued work");
	}
}
