package works.bosk.drivers;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import works.bosk.BoskDiagnosticContext;
import works.bosk.BoskDriver;
import works.bosk.Identifier;
import works.bosk.MapValue;
import works.bosk.Reference;
import works.bosk.StateTreeNode;
import works.bosk.driver.DriverFactory;
import works.bosk.exceptions.InvalidTypeException;

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
public class BufferingDriver implements BoskDriver {
	private final BoskDriver downstream;
	private final Deque<Consumer<BoskDriver>> updateQueue = new ConcurrentLinkedDeque<>();

	public static BufferingDriver writingTo(BoskDriver downstream) {
		return new BufferingDriver(downstream);
	}

	public static DriverFactory factory() {
		return DriverFactory.of(BufferingDriver::new);
	}

	@Override
	public StateTreeNode initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		return downstream.initialRoot(rootType);
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		enqueue(d -> d.submitReplacement(target, newValue), target.root().diagnosticContext());
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		enqueue(d -> d.submitInitialization(target, newValue), target.root().diagnosticContext());
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		enqueue(d -> d.submitDeletion(target), target.root().diagnosticContext());
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		enqueue(d -> d.submitConditionalReplacement(target, newValue, precondition, requiredValue), target.root().diagnosticContext());
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		enqueue(d -> d.submitConditionalDeletion(target, precondition, requiredValue), target.root().diagnosticContext());
	}

	@Override
	public void flush() throws InterruptedException, IOException {
		for (Consumer<BoskDriver> update = updateQueue.pollFirst(); update != null; update = updateQueue.pollFirst()) {
			update.accept(downstream);
		}
		downstream.flush();
	}

	private void enqueue(Consumer<BoskDriver> action, BoskDiagnosticContext diagnosticContext) {
		MapValue<String> capturedAttributes = diagnosticContext.getAttributes();
		updateQueue.add(d -> {
			try (var __ = diagnosticContext.withOnly(capturedAttributes)) {
				action.accept(d);
			}
		});
	}

}
