package io.vena.bosk.updates;

import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;

/**
 * Tragically, switch patterns are still a preview feature in Java 17.
 * Java 17 code that wants to deal with {@link Update} objects polymorphically
 * can use this to tide them over until they can move to Java 21.
 */
public interface UpdateVisitor<R> {
	<T> R visitReplacement(Reference<T> target, T newValue);
	<T> R visitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue);
	<T> R visitInitialization(Reference<T> target, T newValue);
	<T> R visitDeletion(Reference<T> target);
	<T> R visitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue);

	default <T> R visit(Update<T> update) {
		if (update instanceof Replace<T> u) {
			return visitReplacement(u.target(), u.newValue());
		} else if (update instanceof Delete<T> u) {
			return visitDeletion(u.target());
		} else if (update instanceof ConditionalUpdate<T> c) {
			if (c.precondition() instanceof IfEquals p) {
				if (c.action() instanceof Replace<T> u) {
					return visitConditionalReplacement(u.target(), u.newValue(), p.location(), p.requiredValue());
				} else if (c.action() instanceof Delete<T> u) {
					return visitConditionalDeletion(u.target(), p.location(), p.requiredValue());
				} else {
					throw new AssertionError("Unexpected action for IfEquals: " + c.getClass().getSimpleName());
				}
			} else if (c.precondition() instanceof IfNonexistent p) {
				if (c.action() instanceof Replace<T> u) {
					return visitInitialization(u.target(), u.newValue());
				} else {
					throw new AssertionError("Unexpected action for IfNonexistent: " + c.getClass().getSimpleName());
				}
			} else {
				throw new AssertionError("Unexpected precondition type: " + c.precondition().getClass().getSimpleName());
			}
		} else {
			throw new AssertionError("Unexpected update type: " + update.getClass().getSimpleName());
		}
	}
}
