package works.bosk.json.codec.io;

import java.nio.channels.ReadableByteChannel;
import works.bosk.json.mapping.Token;

/**
 * A streaming JSON reader abstraction for high-performance parsing.
 * Designed to work on top of an overlapped buffer fetcher.
 */
public sealed interface JsonReader extends AutoCloseable permits JsonReaderImpl {

	/**
	 * @return a new JsonReader that reads from the given channel.
	 * The channel will be closed when the reader is closed.
	 */
	static JsonReader create(ReadableByteChannel channel) {
		return new JsonReaderImpl(channel);
	}

	/**
	 * Indicates what the next token will be, but does not consume it
	 * like {@link #advanceToken} does.
	 * Skips all syntactically insignificant characters first,
	 * including colons and commas.
	 * <p>
	 * This can be called only in circumstances where {@link #advanceToken} would also
	 * be valid.
	 * Usually, it is followed by a call to {@link #advanceToken}
	 * to actually consume the token.
	 * <p>
	 * This method is idempotent; calling it repeatedly will return the same result.
	 */
	Token peekToken();

	/**
	 * Advances to the next token in the JSON stream.
	 * Skips all syntactically insignificant characters first,
	 * including colons and commas.
	 * <p>
	 * If this returns {@link Token#NUMBER}, call {@link #numberChars} next
	 * to get the character data before calling {@code advanceToken} again.
	 * <p>
	 * If this returns {@link Token#STRING}, call {@link #stringCharacterReader} next
	 * and use it to read the string's character data before calling {@code advanceToken} again.
	 * <p>
	 * All other tokens stand alone; there is no additional data to read.
	 */
	Token advanceToken();

	/**
	 * After {@link #advanceToken} returns NUMBER,
	 * this returns the character data
	 * comprising the text representation of the number.
	 * <p>
	 * Consumes the number from the input, leaving the reader
	 * ready for the next call to {@link #advanceToken}.
	 */
	CharSequence numberChars();

	/**
	 * After {@link #advanceToken} returns STRING,
	 * this returns the characters comprising the string's value.
	 * Useful when there are opportunities to inspect only parts of the string's contents,
	 * such as when it's known to be an element of a fixed set of strings,
	 * leading to highly efficient parsing.
	 * <p>
	 * Consumes the string input, leaving the reader
	 * ready for the next call to {@link #advanceToken}.
	 *
	 * @see #readStringContents()
	 */
	JsonStringCharacterReader stringCharacterReader();

	/**
	 * After {@link #advanceToken} STRING, returns the string's value as a CharSequence.
	 * Handy if you need the entire string.
	 * <p>
	 * Consumes the string input, leaving the reader
	 * ready for the next call to {@link #advanceToken}.
	 *
	 * @see #stringCharacterReader()
	 */
	default CharSequence readStringContents() {
		JsonStringCharacterReader sr = stringCharacterReader();
		StringBuilder sb = new StringBuilder();
		int c;
		while ((c = sr.nextChar()) != -1) {
			sb.appendCodePoint(c);
		}
		return sb;
	}

	default String readString() {
		return readStringContents().toString();
	}

	@Override void close(); // No throws Exception

}
