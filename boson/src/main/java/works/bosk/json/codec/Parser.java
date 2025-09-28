package works.bosk.json.codec;

import java.io.IOException;

public interface Parser {
	Object parse(CharArrayReader json) throws IOException;
}
