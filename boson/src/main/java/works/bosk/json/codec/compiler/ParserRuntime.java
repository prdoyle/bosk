package works.bosk.json.codec.compiler;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import works.bosk.json.codec.CharArrayReader;
import works.bosk.json.codec.ParserSessionImpl;
import works.bosk.json.mapping.Token;

import static java.util.Objects.requireNonNull;
import static works.bosk.json.mapping.Token.INSIGNIFICANT;

/**
 * Generated parser classes extend this.
 * It represents a single parse session, parsing a given {@link CharArrayReader}.
 */
public abstract class ParserRuntime extends ParserSessionImpl {
	private static final AtomicLong curriedArrayCounter = new AtomicLong(0);
	private static final Map<Long, Object[]> curriedArrays = new HashMap<>();

	protected ParserRuntime(CharArrayReader input) {
		super(input);
	}

	public static long curry(Object[] objects) {
		long key = curriedArrayCounter.incrementAndGet();
		curriedArrays.put(key, objects);
		return key;
	}

	protected static Object[] claimCurriedArray(long key) {
		return requireNonNull(curriedArrays.remove(key));
	}

	/**
	 * When positioned at either the start of a token or an {@link Token#INSIGNIFICANT},
	 * advance to the next character that is not insignificant and return its token.
	 */
	protected Token nextToken() throws IOException {
		logEntry("nextToken");
		Token result;
		do {
			int read = read();
			result = Token.startingWith(read);
		} while (result == INSIGNIFICANT);
		return result;
	}

}
