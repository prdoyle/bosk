package io.vena.bosk.updates;

import io.vena.bosk.Catalog;
import io.vena.bosk.Listing;
import io.vena.bosk.Reference;
import io.vena.bosk.SideTable;

/**
 * Requests that the object referenced by <code>target</code> be deleted.
 * The object must be deletable; it must be an entry in a {@link Catalog}, {@link Listing},
 * or {@link SideTable}; or else it must be an {@link java.util.Optional} in which case
 * it will be changed to {@link java.util.Optional#empty()}.
 */
public record Delete<T>(
	Reference<T> target
) implements UnconditionalUpdate<T> { }
