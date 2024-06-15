package io.vena.bosk;

import io.vena.bosk.annotations.Hook;
import io.vena.bosk.annotations.ReferencePath;
import io.vena.bosk.drivers.ReplicaSet;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.stream.IntStream;
import lombok.RequiredArgsConstructor;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.vena.bosk.MultiBoskExternalControllerTest.Status.DONE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests a "cluster" of bosks controlling an external resource.
 * The objective is to perform each control action at least once,
 * but also usually exactly once.
 * Without this ability, we can't scale the capacity of the cluster
 * by adding nodes, because each node will do all the work.
 */
@Disabled("WIP")
public class MultiBoskExternalControllerTest {
	ReplicaSet<BoskState> replicaSet;
	static final int NUM_BOSKS = 1;
	static final int NUM_EVENTS = 4;
	List<Bosk<BoskState>> bosks;

	@Test
	void test() throws InvalidTypeException, IOException, InterruptedException {
		LOGGER.debug("Initialize the replica set");
		replicaSet = new ReplicaSet<>();
		bosks = new ArrayList<>();
		for (int i = 0; i < NUM_BOSKS; i++) {
			bosks.add(new Bosk<BoskState>(
				MultiBoskExternalControllerTest.class.getSimpleName() + "_" + i,
				BoskState.class,
				this::defaultRoot,
				replicaSet.driverFactory()
			));
		}

		LOGGER.debug("Register EventSender hooks");
		Semaphore done = new Semaphore(0);
		Queue<ControlEvent> receivedEvents = new ConcurrentLinkedQueue<>();
		for (var bosk: bosks) {
			bosk.registerHooks(new EventSender(receivedEvents, done));
		}
		var broadcastBosk = bosks.get(0);

		LOGGER.debug("Request the events");
		Refs refs = broadcastBosk.rootReference().buildReferences(Refs.class);
		Catalog<ControlEvent> desiredEvents = Catalog.of(
			IntStream.rangeClosed(1, NUM_EVENTS)
				.mapToObj(ControlEvent::number));
		broadcastBosk.driver().submitReplacement(refs.requestedEvents(), desiredEvents);
		broadcastBosk.driver().flush();

		LOGGER.debug("Wait");
		if (done.tryAcquire(5, SECONDS)) {
			assertEquals(receivedEvents, desiredEvents.asCollection());
		} else {
			fail("Semaphore timed out");
		}
	}

	@RequiredArgsConstructor
	public static class EventSender {
		final Queue<ControlEvent> queue;
		final Semaphore done;

		@Hook("/requestedEvents/-event-")
		void eventChanged(Reference<ControlEvent> ref) {
			ref.forEachValue(queue::add);
		}

		@Hook("/tickets")
		void ticketsChanged(SideTableReference<ControlEvent, Ticket> ref) {
			if (ref.exists()) {
				var table = ref.value();
				if (table.size() == NUM_BOSKS && table.values().stream().allMatch(Ticket::isDone)) {
					done.release();
				}
			}
		}
	}

	private BoskState defaultRoot(BoskInfo<BoskState> bosk) throws InvalidTypeException {
		Refs refs = bosk.rootReference().buildReferences(Refs.class);
		return new BoskState(
			Identifier.from("0"),
			Catalog.empty(),
			SideTable.empty(refs.requestedEvents())
		);
	}

	public interface Refs {
		@ReferencePath("/requestedEvents") CatalogReference<ControlEvent> requestedEvents();
		@ReferencePath("/tickets/-event-") Reference<Ticket> ticket(Identifier event);
	}

	public record BoskState(
		Identifier epoch,
		Catalog<ControlEvent> requestedEvents,
		SideTable<ControlEvent, Ticket> tickets
	) implements StateTreeNode { }

	public record Ticket(
		Identifier owningInstance,
		Status status
	) implements StateTreeNode {
		boolean isDone() { return status == DONE; }
	}

	public enum Status { STARTED, DONE }

	public record ControlEvent(
		Identifier id
	) implements Entity {
		public static ControlEvent number(int n) {
			return new ControlEvent(Identifier.from(Integer.toString(n)));
		}
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(MultiBoskExternalControllerTest.class);
}
