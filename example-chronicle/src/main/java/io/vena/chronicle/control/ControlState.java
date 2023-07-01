package io.vena.chronicle.control;

import io.vena.bosk.Entity;
import io.vena.bosk.Identifier;

public record ControlState(
	Identifier id
) implements Entity { }
