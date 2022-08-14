package org.vena.bosk.drivers;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import org.vena.bosk.BoskDriver;
import org.vena.bosk.Entity;
import org.vena.bosk.Identifier;
import org.vena.bosk.Reference;
import org.vena.bosk.exceptions.InvalidTypeException;

@RequiredArgsConstructor
public class ForwardingDriver<R extends Entity> implements BoskDriver<R> {
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
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		downstream.forEach(d -> d.submitReplacement(target, newValue));
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		downstream.forEach(d -> d.submitConditionalReplacement(target, newValue, precondition, requiredValue));
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		downstream.forEach(d -> d.submitInitialization(target, newValue));
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		downstream.forEach(d -> d.submitDeletion(target));
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		downstream.forEach(d -> d.submitConditionalDeletion(target, precondition, requiredValue));
	}

	@Override
	public void flush() throws InterruptedException, IOException {
		for (BoskDriver<R> d: downstream) {
			// Note that exceptions from a downstream flush() will abort this loop
			d.flush();
		}
	}
}
