package org.vena.bosk.drivers.dynamo;

import java.io.IOException;
import java.lang.reflect.Type;
import org.vena.bosk.BoskDriver;
import org.vena.bosk.Entity;
import org.vena.bosk.Identifier;
import org.vena.bosk.Reference;
import org.vena.bosk.exceptions.InvalidTypeException;
import org.vena.bosk.exceptions.NotYetImplementedException;

public class DynamoDriver<R extends Entity> implements BoskDriver<R> {
	public DynamoDriver(BoskDriver<R> downstream, DynamoDriverSettings settings) {
	}

	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		throw new NotYetImplementedException();
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		throw new NotYetImplementedException();
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		throw new NotYetImplementedException();
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		throw new NotYetImplementedException();
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		throw new NotYetImplementedException();
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		throw new NotYetImplementedException();
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		throw new NotYetImplementedException();
	}

	public void close() {

	}
}
