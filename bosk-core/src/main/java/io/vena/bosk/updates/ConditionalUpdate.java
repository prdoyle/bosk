package io.vena.bosk.updates;

import io.vena.bosk.Reference;


/**
 * Has the same effect as <code>action</code> if the <code>precondition</code> is satisfied
 * at the time the change is applied; otherwise, is silently ignored.
 */
public record ConditionalUpdate<T>(
	Precondition precondition,
	UnconditionalUpdate<T> action
) implements Update<T> {
	@Override public Reference<T> target() { return action.target(); }
}
