package io.vena.bosk;

import io.vena.bosk.drivers.MirroringDriver;
import io.vena.bosk.exceptions.InvalidTypeException;
import lombok.Value;
import org.junit.jupiter.api.Test;

public class QueueTest {

	@Value
	public static class State implements Entity {
		Identifier id;
		Catalog<Task> tasks;
		Phantom<Catalog<Worker>> workers;
	}

	@Value
	public static class Task implements Entity {
		Identifier id;
		Long seed;
	}

	@Value
	public static class Worker implements Entity {
		Identifier id;
	}

	public static class QueueBosk extends Bosk<State> {

		public QueueBosk(DriverFactory<State> driverFactory) throws InvalidTypeException {
			super(
				"QueueBosk",
				State.class,
				new State(Identifier.from("root"), Catalog.empty(), Phantom.empty()),
				driverFactory);
		}

		final CatalogReference<Worker> workersRef = catalogReference(Worker.class, Path.parse(
			"/workers"));

		static QueueBosk create(DriverFactory<State> driverFactory) {
			try {
				return new QueueBosk(driverFactory);
			} catch (InvalidTypeException e) {
				throw new AssertionError(e);
			}
		}
	}

	@Test
	void test() {

		QueueBosk bosk = QueueBosk.create(
			DriverStack.of(
				MirroringDriver.targeting()
			)
		);
	}

}
