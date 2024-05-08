package io.vena.bosk.updates;

import io.vena.bosk.Reference;

public sealed interface Precondition permits IfEquals, IfNonexistent {
	Reference<?> location();
}
