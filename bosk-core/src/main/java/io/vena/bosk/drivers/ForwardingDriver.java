package io.vena.bosk.drivers;

import io.vena.bosk.BoskDriver;
import io.vena.bosk.StateTreeNode;
import io.vena.bosk.exceptions.InvalidTypeException;
import io.vena.bosk.updates.Update;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ForwardingDriver<R extends StateTreeNode> implements BoskDriver<R> {
	private final Iterable<BoskDriver<R>> downstream;

	/**
	 * @return The result of calling <code>initialRoot</code> on the first downstream driver
	 * that doesn't throw {@link UnsupportedOperationException}. Other exceptions are propagated as-is,
	 * and abort the initialization immediately.
	 */
	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		List<UnsupportedOperationException> exceptions = new ArrayList<>();
		for (BoskDriver<R> d: downstream) {
			try {
				return d.initialRoot(rootType);
			} catch (UnsupportedOperationException e) {
				exceptions.add(e);
			}
		}

		// Oh dear.
		UnsupportedOperationException exception = new UnsupportedOperationException("Unable to forward initialRoot request");
		exceptions.forEach(exception::addSuppressed);
		throw exception;
	}

	@Override
	public <T> void submit(Update<T> update) {
		downstream.forEach(d -> d.submit(update));
	}

	@Override
	public void flush() throws InterruptedException, IOException {
		for (BoskDriver<R> d: downstream) {
			// Note that exceptions from a downstream flush() will abort this loop
			d.flush();
		}
	}

	@Override
	public String toString() {
		return "ForwardingDriver{" +
			"downstream=" + downstream +
			'}';
	}
}
