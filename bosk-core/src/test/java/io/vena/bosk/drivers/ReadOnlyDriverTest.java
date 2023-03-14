package io.vena.bosk.drivers;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ReadOnlyDriverTest extends AbstractDriverTest {
	@BeforeEach
	void setup() {
		setupBosksAndReferences(ReadOnlyDriver.factory());
	}

	@Test
	void initialRoot() {
	}

	@Test
	void flush() {
	}

	@Test
	void submitReplacement_throws() {
	}

	@Test
	void submitConditionalReplacement_throws() {
	}

	@Test
	void submitInitialization_throws() {
	}

	@Test
	void submitDeletion_throws() {
	}

	@Test
	void submitConditionalDeletion_throws() {
	}
}
