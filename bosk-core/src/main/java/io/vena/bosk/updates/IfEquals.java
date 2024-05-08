package io.vena.bosk.updates;

import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;

/**
 * A {@link Precondition} that is satisfied if and only if the node at <code>location</code> exists
 * and equals <code>requiredValue</code>.
 */
public record IfEquals(
	Reference<Identifier> location,
	Identifier requiredValue
) implements Precondition { }
