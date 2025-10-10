package works.bosk.json.codec.io;

import java.nio.channels.ReadableByteChannel;

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
	 * Advances to the next token in the JSON stream.
	 * Skips all syntactically insignificant characters,
	 * including colons and commas.
	 * @return the token type
	 */
	Token nextToken();

	/**
	 * After {@link #nextToken} NUMBER, returns the character data
	 * comprising the text representation of the number.
	 */
	CharSequence numberChars();

	/**
	 * After {@link #nextToken} STRING, returns the characters comprising the string's value.
	 * Useful when there are opportunities to inspect only parts of the string's contents,
	 * such as when it's known to be an element of a fixed set of strings,
	 * leading to highly efficient parsing.
	 * @see #readStringContents()
	 */
	JsonStringCharacterReader stringCharacterReader();

	/**
	 * After {@link #nextToken} STRING, returns the string's value as a CharSequence.
	 * Handy if you need the entire string.
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
