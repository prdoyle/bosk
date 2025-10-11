package works.bosk.json.codec;

import works.bosk.json.codec.io.CharArrayJsonReader;
import works.bosk.json.codec.io.JsonStringCharacterReaderImpl;

/**
 * Reads characters out of a JSON literal string value.
 * This class handles UTF-8 decoding and escape sequences,
 * returning the characters that should appear in the string's value,
 * as opposed to its JSON representation.
 * <p>
 * Using this to process string characters has the effect of
 * consuming those characters from the underlying {@link JsonReader}.
 * <p>
 * Notably, this is the only place in JSON where non-ASCII characters can appear.
 */
public sealed interface JsonStringCharacterReader permits CharArrayJsonReader.StringCharacterReader, JsonStringCharacterReaderImpl {

	/**
	 * Advances to the next character (code point) in the string.
	 * @return next decoded code point of the string,
	 * or -1 to indicate the end of the string,
	 * at which point the closing quote has been consumed from the input.
	 */
	int nextChar();

	/**
	 * Skips exactly n decoded code points, handling escape sequences.
	 * Useful for skipping known portions of a string.
	 * @param n number of characters to skip
	 * @throws IllegalArgumentException if {@code n} is negative
	 * @throws IllegalStateException if {@code n} is more than the remaining characters in the string
	 */
	void skipChars(int n);

	/**
	 * Skips the remainder of the string token, as though {@link #nextChar()}
	 * had been called repeatedly until it returned -1.
	 * Useful if the rest of the string's value isn't needed.
	 */
	void skipToEnd();
}
