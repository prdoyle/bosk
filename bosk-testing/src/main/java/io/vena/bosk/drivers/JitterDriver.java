package io.vena.bosk.drivers;

import io.vena.bosk.BoskDriver;
import io.vena.bosk.DriverFactory;
import io.vena.bosk.StateTreeNode;
import io.vena.bosk.exceptions.InvalidTypeException;
import io.vena.bosk.updates.Update;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Random;
import java.util.function.DoubleSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JitterDriver<R extends StateTreeNode> implements BoskDriver<R> {
	private final BoskDriver<R> downstream;
	private final DoubleSupplier jitter;

	public static <RR extends StateTreeNode> DriverFactory<RR> factory(double meanMillis, double limitMillis, long seed) {
		return (b,d) -> new JitterDriver<>(d, meanMillis, limitMillis, seed);
	}

	private JitterDriver(BoskDriver<R> downstream, double meanMillis, double limitMillis, long seed) {
		this.downstream = downstream;
		Random random = new Random(seed);

		https://en.wikipedia.org/wiki/Exponential_distribution#Random_variate_generation
		jitter = ()-> Double.min(limitMillis,
			-Math.log(random.nextDouble()) * meanMillis
		);
	}

	private void sleep() {
		try {
			long totalNanos = (long)(1e6 * jitter.getAsDouble());
			long ms = totalNanos / 1_000_000;
			int nanos = (int) (totalNanos % 1_000_000);
			LOGGER.trace("Sleeping for {} ms", totalNanos/1e6);
			Thread.sleep(ms, nanos);
			LOGGER.trace("Done sleeping");
		} catch (InterruptedException e) {
			LOGGER.debug("Sleep interrupted", e);
		}
	}

	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		sleep();
		return downstream.initialRoot(rootType);
	}

	@Override
	public <T> void submit(Update<T> update) {
		sleep();
		downstream.submit(update);
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		sleep();
		downstream.flush();
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(JitterDriver.class);
}
