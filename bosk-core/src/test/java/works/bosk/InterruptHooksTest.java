package works.bosk;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import works.bosk.annotations.ReferencePath;
import works.bosk.exceptions.InvalidTypeException;
import works.bosk.libtesting.AbstractBoskTest;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static works.bosk.OverloadController.Mode.OVERLOAD;
import static works.bosk.OverloadController.Mode.STANDARD;

public class InterruptHooksTest extends AbstractBoskTest {
	public interface Refs {
		@ReferencePath("/entities/parent/children/-child-")
		Reference<TestChild> child(Identifier child);

		@ReferencePath("/entities/parent/children/-child-/string")
		Reference<String> childString(Identifier child);
	}

	private static final Identifier CHILD_ID = Identifier.from("newChild");

	@Test
	void interruptHooks_interruptsTheRunningHookWithoutInterruptingTheDrainer() throws InterruptedException, InvalidTypeException {
		Bosk<TestRoot> bosk = newBosk("interrupt-running", 1000);
		Refs refs = bosk.rootReference().buildReferences(Refs.class);
		Reference<TestChild> childRef = refs.child(CHILD_ID);

		CountDownLatch hookStarted = new CountDownLatch(1);
		CountDownLatch hookInterrupted = new CountDownLatch(1);
		AtomicBoolean hookTerminated = new AtomicBoolean(false);
		bosk.hookRegistrar().registerHook("blocking", childRef, _ -> {
			hookStarted.countDown();
			try {
				new CountDownLatch(1).await(30, SECONDS);
			} catch (InterruptedException e) {
				hookInterrupted.countDown();
				throw e;
			} finally {
				hookTerminated.set(true);
			}
		});

		Thread drainer = new Thread(() -> bosk.driver().submitConditionalCreation(
			childRef,
			new TestChild(CHILD_ID, "v", TestEnum.OK, Catalog.empty())));
		drainer.setDaemon(true);
		drainer.start();
		assertTrue(hookStarted.await(10, SECONDS), "The hook should start");

		bosk.interruptHooks();

		assertTrue(hookInterrupted.await(10, SECONDS), "The running hook should receive the interrupt");
		drainer.join(10_000);
		assertFalse(drainer.isAlive(), "The draining thread should return");
		assertTrue(hookTerminated.get(), "The hook should terminate before the drainer returns");
		assertFalse(drainer.isInterrupted(), "interruptHooks should not interrupt the thread that was draining");
	}

	@Test
	void interruptHooks_discardsQueuedWorkButKeepsAppliedUpdates() throws InterruptedException, InvalidTypeException {
		Bosk<TestRoot> bosk = newBosk("interrupt-queued", 1000);
		Refs refs = bosk.rootReference().buildReferences(Refs.class);
		Reference<TestChild> childRef = refs.child(CHILD_ID);
		Reference<String> stringRef = refs.childString(CHILD_ID);

		AtomicInteger calls = new AtomicInteger();
		List<String> seen = new CopyOnWriteArrayList<>();
		CountDownLatch hookStarted = new CountDownLatch(1);
		CountDownLatch hookInterrupted = new CountDownLatch(1);
		bosk.hookRegistrar().registerHook("blocking", childRef, ref -> {
			seen.add(ref.valueIfExists().string());
			if (calls.incrementAndGet() == 1) {
				hookStarted.countDown();
				try {
					new CountDownLatch(1).await(30, SECONDS);
				} catch (InterruptedException e) {
					hookInterrupted.countDown();
					throw e;
				}
			}
		});

		Thread drainer = new Thread(() -> bosk.driver().submitConditionalCreation(
			childRef,
			new TestChild(CHILD_ID, "v0", TestEnum.OK, Catalog.empty())));
		drainer.setDaemon(true);
		drainer.start();
		assertTrue(hookStarted.await(10, SECONDS), "The first hook call should start");

		// These are queued behind the blocked hook while the drainer holds the permit.
		bosk.driver().submitReplacement(stringRef, "v1");
		bosk.driver().submitReplacement(stringRef, "v2");

		bosk.interruptHooks();
		assertTrue(hookInterrupted.await(10, SECONDS), "The running hook should be interrupted");
		drainer.join(10_000);
		assertFalse(drainer.isAlive(), "The draining thread should return");

		assertEquals(1, calls.get(), "Queued hook calls should be discarded");
		try (var _ = bosk.readSession()) {
			assertEquals("v2", stringRef.value(), "The queued state updates should still be applied");
		}

		bosk.driver().submitReplacement(stringRef, "v3");
		assertEquals(2, calls.get(), "Hook delivery should resume after the interrupt");
		assertEquals(List.of("v0", "v3"), seen, "The hook should see the creation and the post-interrupt update");
	}

	@Test
	void interruptHooks_whenIdleDoesNotDiscardLaterWork() throws InvalidTypeException {
		Bosk<TestRoot> bosk = newBosk("interrupt-idle", 1000);
		Refs refs = bosk.rootReference().buildReferences(Refs.class);
		Reference<String> stringRef = refs.childString(Identifier.from("child1"));

		List<String> seen = new CopyOnWriteArrayList<>();
		bosk.hookRegistrar().registerHook("recording", stringRef, ref -> seen.add(ref.valueIfExists()));
		seen.clear();

		bosk.interruptHooks();
		bosk.driver().submitReplacement(stringRef, "after");

		assertEquals(List.of("after"), seen, "Hook delivery should work after an idle interrupt");
		assertEquals(STANDARD, bosk.hookMode());
	}

	@Test
	void interruptHooks_clearsOverloadBacklog() throws InterruptedException, InvalidTypeException {
		Bosk<TestRoot> bosk = newBosk("interrupt-overload", 1);
		Refs refs = bosk.rootReference().buildReferences(Refs.class);
		Reference<TestChild> childRef = refs.child(CHILD_ID);
		Reference<String> stringRef = refs.childString(CHILD_ID);

		AtomicInteger calls = new AtomicInteger();
		List<String> seen = new CopyOnWriteArrayList<>();
		CountDownLatch hookStarted = new CountDownLatch(1);
		CountDownLatch hookInterrupted = new CountDownLatch(1);
		bosk.hookRegistrar().registerHook("blocking", childRef, ref -> {
			seen.add(ref.valueIfExists().string());
			if (calls.incrementAndGet() == 1) {
				hookStarted.countDown();
				try {
					new CountDownLatch(1).await(30, SECONDS);
				} catch (InterruptedException e) {
					hookInterrupted.countDown();
					throw e;
				}
			}
		});

		Thread drainer = new Thread(() -> bosk.driver().submitConditionalCreation(
			childRef,
			new TestChild(CHILD_ID, "v0", TestEnum.OK, Catalog.empty())));
		drainer.setDaemon(true);
		drainer.start();
		assertTrue(hookStarted.await(10, SECONDS), "The first hook call should start");
		assertEquals(OVERLOAD, bosk.hookMode(), "The threshold of one should have entered overload mode");

		bosk.driver().submitReplacement(stringRef, "v1");
		bosk.driver().submitReplacement(stringRef, "v2");

		bosk.interruptHooks();
		assertTrue(hookInterrupted.await(10, SECONDS), "The running hook should be interrupted");
		drainer.join(10_000);
		assertFalse(drainer.isAlive(), "The draining thread should return");

		assertEquals(STANDARD, bosk.hookMode(), "The interrupt should reset the overload mode");
		assertEquals(1, calls.get(), "The compacted backlog should be discarded");

		bosk.driver().submitReplacement(stringRef, "v3");
		assertEquals(2, calls.get(), "Hook delivery should resume after clearing the backlog");
		assertEquals(List.of("v0", "v3"), seen, "The hook should see only the creation and the post-interrupt update");
	}

	@Test
	void interruptHooks_stopsRecursiveHook() throws InterruptedException, InvalidTypeException {
		Bosk<TestRoot> bosk = newBosk("interrupt-recursive", 100);
		Refs refs = bosk.rootReference().buildReferences(Refs.class);
		Reference<String> stringRef = refs.childString(Identifier.from("child1"));
		AtomicInteger calls = new AtomicInteger();
		AtomicBoolean armed = new AtomicBoolean(false);
		bosk.hookRegistrar().registerHook("recursive", stringRef, ref -> {
			if (armed.get()) {
				bosk.driver().submitReplacement(ref, "v" + calls.incrementAndGet());
			}
		});
		armed.set(true);

		Thread drainer = new Thread(() -> bosk.driver().submitReplacement(stringRef, "seed"));
		drainer.setDaemon(true);
		drainer.start();
		assertTrue(waitUntil(Duration.ofSeconds(10), () -> calls.get() > 0), "The recursive hook should start");

		bosk.interruptHooks();
		drainer.join(10_000);
		assertFalse(drainer.isAlive(), "The recursive drainer should stop after the interrupt");

		int callsAfterStop = calls.get();
		Thread.sleep(100);
		assertEquals(callsAfterStop, calls.get(), "The recursive hook should not run again after the interrupt");

		Thread restarted = new Thread(() -> bosk.driver().submitReplacement(stringRef, "restart"));
		restarted.setDaemon(true);
		restarted.start();
		assertTrue(waitUntil(Duration.ofSeconds(10), () -> calls.get() > callsAfterStop),
			"A later update should trigger the hook again");
		bosk.interruptHooks();
		restarted.join(10_000);
		assertFalse(restarted.isAlive(), "The restarted drainer should stop after the interrupt");
	}

	private static boolean waitUntil(Duration timeout, java.util.function.BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + timeout.toNanos();
		while (!condition.getAsBoolean()) {
			if (System.nanoTime() >= deadline) {
				return false;
			}
			Thread.sleep(10);
		}
		return true;
	}

	private static Bosk<TestRoot> newBosk(String name, int threshold) {
		return new Bosk<>(
			name,
			TestRoot.class,
			AbstractBoskTest::initialState,
			BoskConfig.<TestRoot>builder()
				.hookOverloadThreshold(threshold)
				.build());
	}
}
