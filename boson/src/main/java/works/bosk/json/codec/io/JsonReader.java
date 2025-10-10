package works.bosk.json.codec.io;

/**
 * A streaming JSON reader abstraction for high-performance parsing.
 * Designed to work on top of an overlapped buffer fetcher.
 */
public interface JsonReader {

	/**
	 * Advances to the next token in the JSON stream.
	 * @return the token type
	 */
	Token nextToken();

	/**
	 * Returns the character data of a numeric token without copying.
	 * Caller must only call when type() == NUMBER.
	 */
	CharSequence numberChars();

	/**
	 * Returns a character-level reader for the current JSON string token.
	 */
	JsonStringCharacterReader stringCharacterReader();

	/**
	 * Convenience method to read the entire string token as a Java String.
	 * Handles escape sequences.
	 */
	default String readString() {
		JsonStringCharacterReader sr = stringCharacterReader();
		StringBuilder sb = new StringBuilder();
		int c;
		while ((c = sr.nextChar()) != -1) {
			sb.appendCodePoint(c);
		}
		return sb.toString();
	}

	/**
	 * Token types for JSON parsing.
	 */
	enum Token {
		BEGIN_OBJECT, END_OBJECT,
		BEGIN_ARRAY, END_ARRAY,
		STRING, NUMBER,
		TRUE, FALSE, NULL,
		END_DOCUMENT
	}
}
