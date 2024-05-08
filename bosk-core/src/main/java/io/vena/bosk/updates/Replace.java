package io.vena.bosk.updates;

import io.vena.bosk.Reference;

/**
 * Requests that the object referenced by <code>target</code> be changed to <code>newValue</code>.
 */
public record Replace<T>(
	Reference<T> target,
	T newValue
) implements UnconditionalUpdate<T> {
	@Override
	public String toString() {
		return "Replace{" +
			"target=" + target +
			", newValue=" + newValue.getClass().getSimpleName() +
			'}';
	}
}
