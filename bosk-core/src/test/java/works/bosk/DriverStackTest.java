package works.bosk;

import works.bosk.drivers.ForwardingDriver;
import org.junit.jupiter.api.Test;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class DriverStackTest {
	final BoskDriver<AbstractBoskTest.TestEntity> baseDriver = new ForwardingDriver<>(emptySet());

	@Test
	void emptyStack_returnsDownstream() {
		BoskDriver<AbstractBoskTest.TestEntity> actual = DriverStack.<AbstractBoskTest.TestEntity>of().build(null, baseDriver);
		assertSame(baseDriver, actual);
	}

	@Test
	void stackedDrivers_correctOrder() {
		DriverStack<AbstractBoskTest.TestEntity> stack = DriverStack.of(
			(b,d) -> new TestDriver<>("first", d),
			(b,d) -> new TestDriver<>("second", d)
		);

		TestDriver<AbstractBoskTest.TestEntity> firstDriver = (TestDriver<AbstractBoskTest.TestEntity>) stack.build(null, baseDriver);
		TestDriver<AbstractBoskTest.TestEntity> secondDriver = (TestDriver<AbstractBoskTest.TestEntity>) firstDriver.downstream;
		BoskDriver<AbstractBoskTest.TestEntity> thirdDriver = secondDriver.downstream;

		assertEquals("first", firstDriver.name);
		assertEquals("second", secondDriver.name);
		assertSame(baseDriver, thirdDriver);
	}

	static class TestDriver<R extends Entity> extends ForwardingDriver<R> {
		final String name;
		final BoskDriver<R> downstream;

		public TestDriver(String name, BoskDriver<R> downstream) {
			super(singletonList(downstream));
			this.name = name;
			this.downstream = downstream;
		}

		@Override
		public String toString() {
			return name;
		}
	}
}
