package io.vena.bosk.drivers;

import io.vena.bosk.Bosk;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.DriverFactory;
import io.vena.bosk.Entity;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.io.IOException;
import java.lang.reflect.Type;
import lombok.RequiredArgsConstructor;

import static lombok.AccessLevel.PRIVATE;

@RequiredArgsConstructor(access= PRIVATE)
public class ReadOnlyDriver<R extends Entity> implements BoskDriver<R> {
	private final BoskDriver<R> downstream;

	public static <R extends Entity> ReadOnlyDriverFactory<R> factory() {
		return (b,downstream) -> new ReadOnlyDriver<>(downstream);
	}

	public interface ReadOnlyDriverFactory<RR extends Entity> extends DriverFactory<RR> {
		@Override ReadOnlyDriver<RR> build(Bosk<RR> bosk, BoskDriver<RR> downstream);
	}

	@Override
	public R initialRoot(Type rootType) throws IOException, InterruptedException, InvalidTypeException {
		return downstream.initialRoot(rootType);
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		downstream.flush();
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		throw unsupportedOperation();
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		throw unsupportedOperation();
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		throw unsupportedOperation();
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		throw unsupportedOperation();
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		throw unsupportedOperation();
	}

	private UnsupportedOperationException unsupportedOperation() {
		return new UnsupportedOperationException("Operation not supported");
	}

}
