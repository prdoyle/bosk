package io.vena.bosk.updates;

import io.vena.bosk.BoskDriver;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.StateTreeNode;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.io.IOException;
import java.lang.reflect.Type;
import lombok.RequiredArgsConstructor;

/**
 * Implements {@link #submit} in terms of the older <code>submitXxx</code> methods.
 */
@RequiredArgsConstructor
public final class LegacyShimDriver<R extends StateTreeNode> implements BoskDriver<R> {
	public final BoskDriver<R> downstream;

	private final UpdateVisitor<Void> downstreamSubmitter = new UpdateVisitor<>() {
		@Override
		public <T> Void visitReplacement(Reference<T> target, T newValue) {
			downstream.submitReplacement(target, newValue);
			return null;
		}

		@Override
		public <T> Void visitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
			downstream.submitConditionalReplacement(target, newValue, precondition, requiredValue);
			return null;
		}

		@Override
		public <T> Void visitInitialization(Reference<T> target, T newValue) {
			downstream.submitInitialization(target, newValue);
			return null;
		}

		@Override
		public <T> Void visitDeletion(Reference<T> target) {
			downstream.submitDeletion(target);
			return null;
		}

		@Override
		public <T> Void visitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
			downstream.submitConditionalDeletion(target, precondition, requiredValue);
			return null;
		}
	};

	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		return downstream.initialRoot(rootType);
	}

	@Override
	public <T> void submit(Update<T> update) {
		downstreamSubmitter.visit(update);
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		downstream.flush();
	}
}
