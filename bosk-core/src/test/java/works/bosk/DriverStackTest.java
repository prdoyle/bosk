package works.bosk;

import org.junit.jupiter.api.Test;
import works.bosk.drivers.ForwardingDriver;
import works.bosk.drivers.NoOpDriver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class DriverStackTest {
	final BoskDriver<AbstractBoskTest.TestEntity> baseDriver = new NoOpDriver<>();

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
		TestDriver<AbstractBoskTest.TestEntity> secondDriver = (TestDriver<AbstractBoskTest.TestEntity>) firstDriver.downstream();
		BoskDriver<AbstractBoskTest.TestEntity> thirdDriver = secondDriver.downstream();

		assertEquals("first", firstDriver.name);
		assertEquals("second", secondDriver.name);
		assertSame(baseDriver, thirdDriver);
	}

	static class TestDriver<R extends Entity> extends ForwardingDriver<R> {
		final String name;

		public TestDriver(String name, BoskDriver<R> downstream) {
			super(downstream);
			this.name = name;
		}

		BoskDriver<R> downstream() {
			return downstream;
		}

		@Override
		public String toString() {
			return name;
		}
	}
}
