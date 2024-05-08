package io.vena.bosk.drivers;

import io.vena.bosk.Bosk;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.DriverFactory;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.StateTreeNode;
import io.vena.bosk.exceptions.InvalidTypeException;
import io.vena.bosk.updates.ConditionalUpdate;
import io.vena.bosk.updates.Delete;
import io.vena.bosk.updates.IfEquals;
import io.vena.bosk.updates.IfNonexistent;
import io.vena.bosk.updates.Replace;
import io.vena.bosk.updates.Update;
import io.vena.bosk.updates.UpdateVisitor;
import java.io.IOException;
import java.lang.reflect.Type;
import lombok.RequiredArgsConstructor;

import static java.util.Arrays.asList;
import static lombok.AccessLevel.PRIVATE;

/**
 * Sends events to another {@link Bosk} of the same type.
 */
@RequiredArgsConstructor(access=PRIVATE)
public class MirroringDriver<R extends StateTreeNode> implements BoskDriver<R> {
	private final Bosk<R> mirror;

	/**
	 * Causes updates to be applied both to <code>mirror</code> and to the downstream driver.
	 */
	public static <RR extends StateTreeNode> DriverFactory<RR> targeting(Bosk<RR> mirror) {
		return (boskInfo, downstream) -> new ForwardingDriver<>(asList(
			new MirroringDriver<>(mirror),
			downstream
		));
	}

	/**
	 * Causes updates to be applied only to <code>other</code>.
	 */
	public static <RR extends StateTreeNode> MirroringDriver<RR> redirectingTo(Bosk<RR> other) {
		return new MirroringDriver<>(other);
	}

	@Override
	public R initialRoot(Type rootType) {
		throw new UnsupportedOperationException(MirroringDriver.class.getSimpleName() + " cannot supply an initial root");
	}

	@Override
	public <T> void submit(Update<T> update) {
		mirror.driver().submit(new ReferenceFixer<T>().visit(update));
	}

	@Override
	public void flush() throws InterruptedException, IOException {
		mirror.driver().flush();
	}

	@SuppressWarnings("unchecked")
	private <T> Reference<T> correspondingReference(Reference<T> original) {
		try {
			return (Reference<T>) mirror.rootReference().then(Object.class, original.path());
		} catch (InvalidTypeException e) {
			throw new AssertionError("References are expected to be compatible: " + original, e);
		}
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private class ReferenceFixer<TT> implements UpdateVisitor<Update<TT>> {

		@Override
		public <T> Update<TT> visitReplacement(Reference<T> target, T newValue) {
			return new Replace(correspondingReference(target), newValue);
		}

		@Override
		public <T> Update<TT> visitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
			return new ConditionalUpdate<>(
				new IfEquals(correspondingReference(precondition), requiredValue),
				new Replace(correspondingReference(target), newValue)
			);
		}

		@Override
		public <T> Update<TT> visitInitialization(Reference<T> target, T newValue) {
			var correspondingTarget = correspondingReference(target);
			return new ConditionalUpdate(
				new IfNonexistent(correspondingTarget),
				new Replace<>(correspondingTarget, newValue)
			);
		}

		@Override
		public <T> Update<TT> visitDeletion(Reference<T> target) {
			return new Delete(correspondingReference(target));
		}

		@Override
		public <T> Update<TT> visitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
			return new ConditionalUpdate<>(
				new IfEquals(correspondingReference(precondition), requiredValue),
				new Delete(correspondingReference(target))
			);
		}
	}

	@Override
	public String toString() {
		return "Mirroring to " + mirror;
	}
}
