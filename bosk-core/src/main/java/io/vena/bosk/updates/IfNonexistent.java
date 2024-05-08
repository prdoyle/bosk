package io.vena.bosk.updates;

import io.vena.bosk.Reference;

/**
 * A {@link Precondition} that is satisfied if and only if the node at <code>location</code> does not exist.
 */
public record IfNonexistent(
	Reference<?> location
) implements Precondition { }
