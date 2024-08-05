package works.bosk.hello;

import org.springframework.stereotype.Component;
import works.bosk.Bosk;
import works.bosk.Catalog;
import works.bosk.CatalogReference;
import works.bosk.Identifier;
import works.bosk.annotations.ReferencePath;
import works.bosk.exceptions.InvalidTypeException;
import works.bosk.hello.state.BoskState;
import works.bosk.hello.state.Target;
import works.bosk.logback.BoskLogFilter;
import works.bosk.logback.BoskLogFilter.LogController;

@Component
public class HelloBosk extends Bosk<BoskState> {
	public HelloBosk(LogController logController) throws InvalidTypeException {
		super("Hello", BoskState.class, HelloBosk::defaultRoot, BoskLogFilter.withController(logController));
	}

	public final Refs refs = buildReferences(Refs.class);

	public interface Refs {
		@ReferencePath("/targets") CatalogReference<Target> targets();
	}

	private static BoskState defaultRoot(Bosk<BoskState> bosk) {
		return new BoskState(
			Catalog.of(new Target(Identifier.from("world")))
		);
	}
}
