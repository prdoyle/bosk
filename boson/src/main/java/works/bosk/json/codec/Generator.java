package works.bosk.json.codec;

import java.io.Writer;

public interface Generator {
	void generate(Writer out, Object value);
}
