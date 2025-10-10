package works.bosk.json.codec.io;

/**
 * Reads characters out of a JSON string literal.
 * This is the only place in JSON where non-ASCII characters can appear.
 * This class handles UTF-8 decoding and escape sequences,
 * returning the characters that should appear in the string's value,
 * as opposed to its JSON representation.
 */
public interface JsonStringCharacterReader {

	/**
	 * @return next decoded codepoint of the string,
	 * or -1 to indicate the end of the string,
	 * at which point the closing quote has been consumed.
	 */
	int nextChar();

	/**
	 * Skips exactly n decoded characters, handling escape sequences.
	 * Useful for skipping known portions of a string.
	 */
	void skipChars(int n);

	/**
	 * Skips to the end of the string token.
	 * Useful if the rest of the string's value isn't needed.
	 * @param n the number of characters to skip; the following character must be the closing quote
	 */
	void skipToEnd(int n);
}
