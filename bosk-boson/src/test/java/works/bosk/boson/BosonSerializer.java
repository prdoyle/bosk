package works.bosk.boson;

import java.util.ArrayList;
import java.util.List;
import works.bosk.BoskInfo;
import works.bosk.Catalog;
import works.bosk.Entity;
import works.bosk.Identifier;
import works.bosk.StateTreeSerializer;
import works.bosk.json.mapping.TypeScanner;
import works.bosk.json.mapping.TypeScanner.Directive;
import works.bosk.json.mapping.spec.RepresentAsSpec;
import works.bosk.json.mapping.spec.StringNode;
import works.bosk.json.mapping.spec.UniformMapNode;
import works.bosk.json.types.DataType;
import works.bosk.json.types.TypeReference;

public class BosonSerializer extends StateTreeSerializer {

	public TypeScanner.Bundle bundleFor(BoskInfo<?> bosk) {
		var directives = new ArrayList<>();

		var identifierSpec = RepresentAsSpec.as(
			new StringNode(),
			DataType.known(Identifier.class),
			Identifier::toString,
			Identifier::from
		);
		directives.add(new Directive(
			DataType.known(Identifier.class),
			_ -> identifierSpec
		));

		directives.add(new Directive(
			DataType.of(new TypeReference<Catalog<? extends Entity>>(){}),
			catalogType -> RepresentAsSpec.as(
				new UniformMapNode()
			)
		));
		return new TypeScanner.Bundle(List.copyOf(directives));
	}

}
