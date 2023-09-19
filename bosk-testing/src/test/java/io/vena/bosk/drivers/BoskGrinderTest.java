package io.vena.bosk.drivers;

import io.vena.bosk.Bosk;
import io.vena.bosk.Catalog;
import io.vena.bosk.DriverFactory;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.annotations.ReferencePath;
import io.vena.bosk.drivers.state.TestEntity;
import io.vena.bosk.exceptions.InvalidTypeException;
import io.vena.bosk.exceptions.NotYetImplementedException;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import lombok.var;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public class BoskGrinderTest extends AbstractDriverTest {
	private static final int NUM_THREADS = 10;
	private static final int NUM_ROUNDS = 20;
	private static final int NUM_OPS_PER_ROUND = 50;
	CyclicBarrier barrier = new CyclicBarrier(1 + NUM_THREADS);
	final List<Mutator> mutators;
	volatile boolean isFinished = false;
	Refs refs;

	public interface Refs {
		@ReferencePath("/string") Reference<String> string();
		@ReferencePath("/catalog") Reference<Catalog<TestEntity>> catalog();
		@ReferencePath("/catalog/-id-") Reference<TestEntity> entity(Identifier id);
		@ReferencePath("/catalog/-child-/string") Reference<String> string(Identifier child);
	}

	public BoskGrinderTest() throws InvalidTypeException {
		DriverFactory<TestEntity> factory = DriverStateVerifier.wrap(Bosk::simpleDriver, TestEntity.class, AbstractDriverTest::initialRoot);
		bosk = new Bosk<TestEntity>("Test bosk", TestEntity.class, AbstractDriverTest::initialRoot, factory);
		driver = bosk.driver();
		refs = bosk.buildReferences(Refs.class);
		long seed = 123L;
		mutators = IntStream.rangeClosed(1, NUM_THREADS)
			.mapToObj(i -> new Mutator(seed+i))
			.collect(toList());
	}

	@Test
	public void run() throws InterruptedException, IOException {
		currentThread().setName("Main thread");
		List<Thread> threads = mutators.stream()
			.map(m -> new Thread(m, m.myID.toString()))
			.collect(toList());
		threads.forEach(Thread::start);
		for (int round = 0; round < NUM_ROUNDS; round++) {
			waitUp();
			LOGGER.debug("Round {}: Waiting for mutators", round);
			waitUp();
			LOGGER.debug("Flushing");
			bosk.driver().flush();
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

	class Mutator implements Runnable {
		final Identifier myID;
		final Random random;

		public Mutator(long seed) {
			myID = Identifier.from("m" + seed);
			random = new Random(seed);
		}

		@Override
		public void run() {
			waitUp();
			LOGGER.debug("Started");
			while (!isFinished) {
				LOGGER.debug("Doing random things");
				try {
					doRandomThings();
				} catch (Throwable t) {
					isFinished = true;
					throw t;
				}
				LOGGER.debug("Done random things");
				waitUp();
				LOGGER.debug("Waiting for verification");
				waitUp();
			}
		}

		private void doRandomThings() {
			for (int i = 0; i < NUM_OPS_PER_ROUND; i++) {
				Identifier id;
				if (random.nextInt(100) < 50) {
					id = myID;
				} else {
					// Change another guy's node!
					id = mutators.get(random.nextInt(mutators.size())).myID;
				}
				int actionPct = random.nextInt(100);
				if ((actionPct -= 50) < 0) {
					bosk.driver().submitReplacement(refs.entity(id),
						TestEntity.empty(id, refs.catalog())
							.withString("s" + random.nextInt(1000)));
				} else if ((actionPct -= 40) < 0) {
					bosk.driver().submitReplacement(refs.string(id),
						"s" + random.nextInt(1000));
				} else {
					bosk.driver().submitDeletion(refs.entity(id));
				}
			}
		}

	}

	private void waitUp() {
		if (barrier.isBroken()) {
			return;
		}
		try {
			barrier.await(5, SECONDS);
		} catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
			throw new NotYetImplementedException(e);
		}
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(BoskGrinderTest.class);
}
