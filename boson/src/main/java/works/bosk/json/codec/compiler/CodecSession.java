package works.bosk.json.codec.compiler;

import java.io.IOException;

public interface CodecSession {
	Object parse() throws IOException;
}
