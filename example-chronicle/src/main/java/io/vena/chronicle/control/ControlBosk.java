package io.vena.chronicle.control;

import io.vena.bosk.Bosk;
import io.vena.bosk.Identifier;
import org.springframework.stereotype.Component;

@Component
public class ControlBosk extends Bosk<ControlState> {
	public ControlBosk() {
		super(
			ControlBosk.class.getSimpleName(),
			ControlState.class,
			ControlBosk::defaultRoot,
			Bosk::simpleDriver);
	}

	private static ControlState defaultRoot(Bosk<ControlState> bosk) {
		return new ControlState(
			Identifier.from(ControlBosk.class.getSimpleName()));
	}
}
