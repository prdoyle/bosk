package works.bosk;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BoskConfigTest {
	record TestState() implements StateTreeNode {}

	@Test
	void defaultHookOverloadThreshold() {
		BoskConfig<TestState> config = BoskConfig.<TestState>builder().build();
		assertEquals(1000, config.hookOverloadThreshold(), "The default hook overload threshold should be 1000");
	}

	@Test
	void zeroAndOneHookOverloadThresholdsAreAllowed() {
		assertEquals(0, BoskConfig.<TestState>builder().hookOverloadThreshold(0).build().hookOverloadThreshold(),
			"Zero should be accepted as a threshold");
		assertEquals(1, BoskConfig.<TestState>builder().hookOverloadThreshold(1).build().hookOverloadThreshold(),
			"One should be accepted as a threshold");
	}

	@Test
	void hookOverloadThresholdMustNotBeNegative() {
		assertThrows(IllegalArgumentException.class,
			() -> BoskConfig.<TestState>builder().hookOverloadThreshold(-1).build(),
			"A negative hook overload threshold is not meaningful");
	}
}
