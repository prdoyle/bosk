package io.vena.bosk.updates;

import io.vena.bosk.Reference;

public sealed interface Update<T> permits ConditionalUpdate, UnconditionalUpdate {
	Reference<T> target();
}
