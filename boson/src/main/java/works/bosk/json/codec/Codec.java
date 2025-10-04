package works.bosk.json.codec;

import works.bosk.json.mapping.spec.JsonValueSpec;

public interface Codec {
	Parser parserFor(JsonValueSpec spec);
	Generator generatorFor(JsonValueSpec spec);
}
