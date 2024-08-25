package works.bosk.drivers;

import java.io.IOException;
import java.lang.reflect.Type;
import works.bosk.BoskDriver;
import works.bosk.Identifier;
import works.bosk.Reference;
import works.bosk.StateTreeNode;
import works.bosk.driver.DriverFactory;
import works.bosk.exceptions.InvalidTypeException;

public class NoOpDriver implements BoskDriver {
	public static DriverFactory factory() {
		return DriverFactory.ofInstance(INSTANCE);
	}

	@Override
	public StateTreeNode initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		throw new UnsupportedOperationException();
	}

	@Override public <T> void submitReplacement(Reference<T> target, T newValue) { }
	@Override public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) { }
	@Override public <T> void submitInitialization(Reference<T> target, T newValue) { }
	@Override public <T> void submitDeletion(Reference<T> target) { }
	@Override public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) { }
	@Override public void flush() throws IOException, InterruptedException { }

	private static final NoOpDriver INSTANCE = new NoOpDriver();
}
