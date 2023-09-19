package io.vena.bosk.drivers;

import io.vena.bosk.Bosk;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.Catalog;
import io.vena.bosk.DriverFactory;
import io.vena.bosk.DriverStack;
import io.vena.bosk.Identifier;
import io.vena.bosk.Path;
import io.vena.bosk.Reference;
import io.vena.bosk.StateTreeNode;
import io.vena.bosk.annotations.ReferencePath;
import io.vena.bosk.drivers.state.TestEntity;
import io.vena.bosk.exceptions.InvalidTypeException;
import io.vena.bosk.exceptions.NotYetImplementedException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.IntStream;
import lombok.RequiredArgsConstructor;
import lombok.var;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Thread.currentThread;
import static java.util.stream.Collectors.toList;

public class BoskGrinder extends AbstractDriverTest {
	private static final int NUM_THREADS = 2;
	CyclicBarrier barrier = new CyclicBarrier(1 + NUM_THREADS);
	volatile boolean isFinished = false;
	final Map<Path, Object> pendingReplacements = new ConcurrentHashMap<>();
	Refs refs;

	public interface Refs {
		@ReferencePath("/string") Reference<String> string();
		@ReferencePath("/catalog") Reference<Catalog<TestEntity>> catalog();
		@ReferencePath("/catalog/-id-") Reference<TestEntity> entity(Identifier id);
	}

	public BoskGrinder() throws InvalidTypeException {
		DriverFactory<TestEntity> factory = Bosk::simpleDriver;

		// This is the bosk whose behaviour we'll consider to be correct by definition
		canonicalBosk = new Bosk<TestEntity>("Canonical bosk", TestEntity.class, AbstractDriverTest::initialRoot, DriverStack.of(
			SwitcherooDriver::new
		));

		// This is the bosk we're testing
		bosk = new Bosk<TestEntity>("Test bosk", TestEntity.class, AbstractDriverTest::initialRoot, DriverStack.of(
			// Updates going to the driver we're testing get saved for the sake of Switcheroo
			SavePendingReplacementsDriver::new,
			factory,
			// Updates coming back from the driver we're testing go to the canonical bosk in the order received
			MirroringDriver.targeting(canonicalBosk)
		));
		driver = bosk.driver();
		refs = bosk.buildReferences(Refs.class);
	}

	@Test
	public void run() throws InterruptedException {
		currentThread().setName("Main thread");
		List<Thread> threads = IntStream.rangeClosed(1, NUM_THREADS)
			.mapToObj(i -> new Thread(new Mutator(), "Mutator " + i))
			.collect(toList());
		threads.forEach(Thread::start);
		for (int i = 0; i < 50; i++) {
			waitUp();
			LOGGER.debug("Waiting for mutators");
			waitUp();
			LOGGER.debug("Verifying contents");
			assertCorrectBoskContents();
		}
		isFinished = true;
		waitUp();
		LOGGER.debug("Waiting for threads to finish");
		for (Thread thread : threads) {
			thread.join();
		}
		try (var __ = bosk.readContext()) {
			LOGGER.debug("State:\n{}", bosk.rootReference().value());
		}
	}

	@RequiredArgsConstructor
	class Mutator implements Runnable {
		final Identifier id = Identifier.unique("mutator");

		@Override
		public void run() {
			waitUp();
			LOGGER.debug("Started");
			while (!isFinished) {
				doRandomThings();
				waitUp();
				LOGGER.debug("Waiting for verification");
				waitUp();
			}
		}

		private void doRandomThings() {
			try {
				LOGGER.debug("Doing random things");
				bosk.driver().submitReplacement(refs.entity(id), TestEntity.empty(id, refs.catalog()));
				Thread.sleep(1);
				LOGGER.debug("Done random things");
			} catch (InterruptedException e) {
				throw new NotYetImplementedException(e);
			}
		}

	}

	private void waitUp() {
		try {
			barrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			throw new NotYetImplementedException(e);
		}
	}

	class SavePendingReplacementsDriver<R extends StateTreeNode> implements BoskDriver<R> {
		final BoskDriver<R> downstream;

		public SavePendingReplacementsDriver(Bosk<R> bosk, BoskDriver<R> downstream) {
			this.downstream = downstream;
		}

		@Override
		public <T> void submitReplacement(Reference<T> target, T newValue) {
			pendingReplacements.put(target.path(), newValue);
			downstream.submitReplacement(target, newValue);
		}

		@Override
		public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
			pendingReplacements.put(target.path(), newValue);
			downstream.submitConditionalReplacement(target, newValue, precondition, requiredValue);
		}

		@Override
		public <T> void submitInitialization(Reference<T> target, T newValue) {
			pendingReplacements.put(target.path(), newValue);
			downstream.submitInitialization(target, newValue);
		}

		@Override public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException { return downstream.initialRoot(rootType); }
		@Override public <T> void submitDeletion(Reference<T> target) { downstream.submitDeletion(target); }
		@Override public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) { downstream.submitConditionalDeletion(target, precondition, requiredValue); }
		@Override public void flush() throws IOException, InterruptedException { downstream.flush(); }
	}

	/**
	 * For replacement operations, substitutes the expected value from {@link #pendingReplacements}
	 * rather than accepting the value supplied in the update itself.
	 */
	class SwitcherooDriver<R extends StateTreeNode> implements BoskDriver<R> {
		final BoskDriver<R> downstream;

		public SwitcherooDriver(Bosk<R> bosk, BoskDriver<R> downstream) {
			this.downstream = downstream;
		}

		@Override
		public <T> void submitReplacement(Reference<T> target, T newValue) {
			downstream.submitReplacement(target, replacementFor(target));
		}

		@Override
		public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
			downstream.submitConditionalReplacement(target, replacementFor(target), precondition, requiredValue);
		}

		@Override
		public <T> void submitInitialization(Reference<T> target, T newValue) {
			downstream.submitInitialization(target, replacementFor(target));
		}

		<T> T replacementFor(Reference<T> target) {
			Object result = pendingReplacements.get(target.path());
			return target.targetClass().cast(result);
		}

		// Everything else is just delegated
		@Override public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException { return downstream.initialRoot(rootType); }
		@Override public <T> void submitDeletion(Reference<T> target) { downstream.submitDeletion(target); }
		@Override public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) { downstream.submitConditionalDeletion(target, precondition, requiredValue); }
		@Override public void flush() throws IOException, InterruptedException { downstream.flush(); }
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(BoskGrinder.class);
}
