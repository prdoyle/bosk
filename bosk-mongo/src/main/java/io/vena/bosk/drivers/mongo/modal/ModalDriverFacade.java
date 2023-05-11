package io.vena.bosk.drivers.mongo.modal;

import io.vena.bosk.Bosk;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.DriverFactory;
import io.vena.bosk.Entity;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.drivers.mongo.MongoDriver;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ModalDriverFacade<R extends Entity> implements MongoDriver<R> {
	private final AtomicReference<MongoDriver<R>> currentImplementation;

	public ModalDriverFacade(MongoDriver<R> initialDownstream) {
		// FIXME: This isn't right. ReconnectingDriver should just be waiting for the reconnection,
		// not initiating it. There can (briefly) be more than one ReconnectingDriver object in some
		// concurrent use cases, so the single ModalDriverFacade should initiate the reconnect
		// and the ReconnectingDriver should just wait.
		currentImplementation = new AtomicReference<>(initialDownstream);
	}

	/**
	 * @throws ClassCastException if the downstream driver is not a {@link MongoDriver}.
	 */
	public static <RR extends Entity> Factory<RR> factory() {
		return (b,d) -> new ModalDriverFacade<>((MongoDriver<RR>) d);
	}

	public interface Factory<R extends Entity> extends DriverFactory<R> {
		@Override ModalDriverFacade<R> build(Bosk<R> bosk, BoskDriver<R> downstream);
	}

	public MongoDriver<R> currentImplementation() { return currentImplementation.get(); }

	/**
	 * @return true if successful; false if <code>from</code> doesn't match
	 */
	public boolean changeImplementation(MongoDriver<R> from, MongoDriver<R> to) {
		boolean success = this.currentImplementation.compareAndSet(from, to);
		if (success) {
			LOGGER.trace("Changed implementation from {} to {}", from, to);
		} else {
			LOGGER.debug("Failed to change implementation from {} to {}", from, to);
		}
		return success;
	}

	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		return currentImplementation.get().initialRoot(rootType);
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		currentImplementation.get().submitReplacement(target, newValue);
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		currentImplementation.get().submitConditionalReplacement(target, newValue, precondition, requiredValue);
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		currentImplementation.get().submitInitialization(target, newValue);
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		currentImplementation.get().submitDeletion(target);
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		currentImplementation.get().submitConditionalDeletion(target, precondition, requiredValue);
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		currentImplementation.get().flush();
	}

	@Override
	public void refurbish() {
		currentImplementation.get().refurbish();
	}

	@Override
	public void close() {
		currentImplementation.get().close();
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(ModalDriverFacade.class);
}
