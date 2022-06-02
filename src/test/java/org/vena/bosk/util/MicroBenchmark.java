package org.vena.bosk.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MicroBenchmark {
	private final long duration = 10_000;

	protected abstract void doIterations(long count);

	public double computeRate() {
		LOGGER.info("Warmup");
		for (int i = 1; i <= 5; i++) {
			runFor(duration / 10);
		}
		LOGGER.info("Run");
		double rate = runFor(duration / 2);
		LOGGER.info("Finished");
		return rate;
	}

	private double runFor(long targetDuration) {
		// Log statements are commented out in here so they don't interfere with measurement

		long totalIterations = 0;
		long totalDuration = 0;
		long loopCount = 1;
		while (loopCount > 0) {
			// Do the run
			long startTime = System.currentTimeMillis();
			doIterations(loopCount);
			long endTime = System.currentTimeMillis();
			long elapsedTime = endTime - startTime;
			//LOGGER.debug("{} iterations in {} ms", loopCount, elapsedTime);

			// Update stats
			totalIterations += loopCount;
			totalDuration += elapsedTime;

			if (totalDuration >= targetDuration) {
				break;
			}

			// Choose the next loopCount
			long remainingDuration = targetDuration - totalDuration;
			// We expect loopCount/elapsedTime to equal X/remainingDuration and we want X.
			// Cross multiplying, loopCount * remainingDuration = X * elapsedTime
			// But we also don't want X to be more that double loopCount.
			if (remainingDuration > elapsedTime * 2) {
				loopCount *= 2;
				//LOGGER.debug("Doubled to {} iterations", loopCount);
			} else {
				// Don't want to worry about overflow here
				double idealLoopCount = ((double) loopCount) * remainingDuration / ((double)elapsedTime);
				loopCount = (long)idealLoopCount;
				//LOGGER.debug("Scaled to {} iterations", loopCount);
			}
		}
		//LOGGER.debug("{} iterations in {} ms", totalIterations, totalDuration);
		return 1000.0 * totalIterations / totalDuration; // Per second
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(MicroBenchmark.class);
}
