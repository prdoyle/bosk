package works.bosk.drivers;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import works.bosk.Bosk;
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
 * The primary way to use this class is to instantiate a {@code new ReplicaSet()},
 * then construct any number of bosks using {@link #driverFactory()}.
 * All the resulting bosks will be in the replica set, and more can be added dynamically.
 * <p>
 * There are also some factory methods that simplify some special use cases.
 *
 * <ol>
 *     <li>
 *         Use {@link #mirroringTo} to mirror changes from a primary bosk to some number
 *         of secondary ones.
 *     </li>
 *     <li>
 *         Use {@link #redirectingTo} just to get a driver that can accept references to the wrong bosk.
 *     </li>
 * </ol>
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

	/**
	 * Causes updates to be applied to {@code mirrors} and to the downstream driver.
	 * <p>
	 * Assuming the returned factory is only used once, this has the effect of creating
	 * a fixed replica set to which new replicas can't be added dynamically;
	 * but the returned factory can be used multiple times.
	 */
	@SafeVarargs
	public static <RR extends StateTreeNode> DriverFactory<RR> mirroringTo(Bosk<RR>... mirrors) {
		var replicaSet = new ReplicaSet<RR>();
		for (var m: mirrors) {
			BoskDriver<RR> downstream = m.driver();
			replicaSet.replicas.add(new Replica<>(
				m.rootReference(),
				new ForwardingDriver<>(downstream) {
					@Override
					public RR initialRoot(Type rootType) {
						throw new UnsupportedOperationException("Don't use initialRoot from " + m);
					}

					@Override
					public String toString() {
						return downstream.toString() + " (minus initial state)";
					}
				}
			));
		}
		return replicaSet.driverFactory();
	}

	/**
	 * Causes updates to be applied only to <code>other</code>.
	 */
	public static <RR extends StateTreeNode> BoskDriver<RR> redirectingTo(Bosk<RR> other) {
		// A ReplicaSet with only the one replica
		return new ReplicaSet<RR>()
			.driverFactory().build(
				other,
				other.driver()
			);
	}

	final class BroadcastShim implements BoskDriver<R> {
		/**
		 * TODO: should return the current state somehow. For now, I guess it's best to attach all the bosks before submitting any updates.
		 *
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
