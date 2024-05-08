package io.vena.bosk.drivers;

import io.vena.bosk.BoskDiagnosticContext;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.DriverFactory;
import io.vena.bosk.MapValue;
import io.vena.bosk.StateTreeNode;
import io.vena.bosk.exceptions.InvalidTypeException;
import io.vena.bosk.updates.Update;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import lombok.RequiredArgsConstructor;

import static lombok.AccessLevel.PROTECTED;

/**
 * Queues updates and submits them to a downstream driver when {@link #flush()}
 * is called.
 *
 * <p>
 * This has the effect of causing the whole list of updates to be
 * discarded if an exception is thrown while the updates are being computed,
 * which could be a desirable property. However, the buffered updates are
 * <strong>not</strong> submitted downstream atomically: other updates from other
 * threads may be interleaved. (They are, of course, submitted downstream
 * in the order they were submitted to this driver.)
 *
 * @author pdoyle
 */
@RequiredArgsConstructor(access = PROTECTED)
public class BufferingDriver<R extends StateTreeNode> implements BoskDriver<R> {
	private final BoskDriver<R> downstream;
	private final Deque<Runnable> updateQueue = new ConcurrentLinkedDeque<>();

	public static <RR extends StateTreeNode> BufferingDriver<RR> writingTo(BoskDriver<RR> downstream) {
		return new BufferingDriver<>(downstream);
	}

	public static <RR extends StateTreeNode> DriverFactory<RR> factory() {
		return (b,d) -> new BufferingDriver<>(d);
	}

	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		return downstream.initialRoot(rootType);
	}

	@Override
	public <T> void submit(Update<T> update) {
		BoskDiagnosticContext diagnosticContext = update.target().root().diagnosticContext();
		MapValue<String> capturedAttributes = diagnosticContext.getAttributes();
		updateQueue.add(() -> {
			try (var __ = diagnosticContext.withOnly(capturedAttributes)) {
				downstream.submit(update);
			}
		});
	}

	@Override
	public void flush() throws InterruptedException, IOException {
		for (Runnable update = updateQueue.pollFirst(); update != null; update = updateQueue.pollFirst()) {
			update.run();
		}
		downstream.flush();
	}

}
