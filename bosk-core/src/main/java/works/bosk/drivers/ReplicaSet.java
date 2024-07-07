package works.bosk.drivers;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import works.bosk.BoskDriver;
import works.bosk.DriverFactory;
import works.bosk.Identifier;
import works.bosk.Reference;
import works.bosk.RootReference;
import works.bosk.StateTreeNode;
import works.bosk.exceptions.InvalidTypeException;

/**
 * A pool of bosks arranged such that submitting an update to any of them submits to all of them.
 * New bosks can be added any time by initializing them with {@link #driverFactory()} in their driver stack.
 * <p>
 * Note that this isn't used for true distributed replica sets with one bosk in each JVM process,
 * but rather for constructing a local replica set within a single JVM process.
 * <p>
 * <em>Evolution note</em>: This kind of subsumes the functionality of both {@link ForwardingDriver}
 * and {@link MirroringDriver}. Seems like this class could either use or replace those.
 * Perhaps {@link ForwardingDriver} should do the {@link Replica#correspondingReference} logic that
 * {@link MirroringDriver} does, making the latter unnecessary; and then this class could
 * simply be a {@link ForwardingDriver} whose list of downstream drivers is mutable.
 */
public class ReplicaSet<R extends StateTreeNode> {
	final Queue<Replica<R>> replicas = new ConcurrentLinkedQueue<>();

	/**
	 * We can actually use the same driver for every bosk in the replica set
	 * because they all do the same thing!
	 */
	final BroadcastShim shim = new BroadcastShim();

	public DriverFactory<R> driverFactory() {
		return (b,d) -> {
			replicas.add(new Replica<>(b.rootReference(), d));
			return shim;
		};
	}

	final class BroadcastShim implements BoskDriver<R> {
		/**
		 * @return The result of calling <code>initialRoot</code> on the first downstream driver
		 * that doesn't throw {@link UnsupportedOperationException}. Other exceptions are propagated as-is,
		 * and abort the initialization immediately.
		 */
		@Override
		public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
			List<UnsupportedOperationException> exceptions = new ArrayList<>();
			for (var r: replicas) {
				try {
					return r.driver().initialRoot(rootType);
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
			replicas.forEach(r -> r.driver
				.submitReplacement(
					r.correspondingReference(target), newValue));
		}

		@Override
		public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
			replicas.forEach(r -> r.driver
				.submitConditionalReplacement(
					r.correspondingReference(target), newValue,
					r.correspondingReference(precondition), requiredValue));
		}

		@Override
		public <T> void submitInitialization(Reference<T> target, T newValue) {
			replicas.forEach(r -> r.driver
				.submitInitialization(
					r.correspondingReference(target), newValue));
		}

		@Override
		public <T> void submitDeletion(Reference<T> target) {
			replicas.forEach(r -> r.driver
				.submitDeletion(
					r.correspondingReference(target)));
		}

		@Override
		public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
			replicas.forEach(r -> r.driver
				.submitConditionalDeletion(
					r.correspondingReference(target),
					r.correspondingReference(precondition), requiredValue));
		}

		@Override
		public void flush() throws IOException, InterruptedException {
			for (var r : replicas) {
				r.driver().flush();
			}
		}

	}

	record Replica<R extends StateTreeNode>(
		RootReference<R> root,
		BoskDriver<R> driver
	){
		@SuppressWarnings("unchecked")
		private <T> Reference<T> correspondingReference(Reference<T> original) {
			try {
				return (Reference<T>) root.then(Object.class, original.path());
			} catch (InvalidTypeException e) {
				throw new AssertionError("Every reference should support a target class of Object", e);
			}
		}
	}

}
