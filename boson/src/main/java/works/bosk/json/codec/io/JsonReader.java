package works.bosk.json.codec.io;

import java.nio.channels.ReadableByteChannel;
import works.bosk.json.mapping.Token;

/**
 * A streaming JSON reader abstraction for high-performance parsing.
 * This interface is rather unfriendly by design.
 * Methods mutate the reader's internal state in ways that are not obvious
 * from the outside.
 * The rather stringent rules are documented here, and if you don't follow them,
 * you will get confusing behaviour.
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
	 * Skips insignificant characters and returns the next token encountered.
	 * <p>
	 * Depending on the token returned, the next method called must be one of the following:
	 * <ul>
	 *     <li>
	 *         for any token with a {@link Token#fixedRepresentation fixed representation},
	 *         call {@link #consumeFixedToken};
	 *     </li>
	 *     <li>
	 *         for {@link Token#NUMBER}, call {@link #consumeNumber}; or
	 *     </li>
	 *     <li>
	 *         for {@link Token#STRING}, call {@link #consumeString}.
	 *     </li>
	 * </ul>
	 *
	 * This method is idempotent; calling it repeatedly will return the same result.
	 */
	Token peekToken();

	/**
	 * After {@link #peekToken} returns a token with a {@link Token#hasFixedRepresentation fixed representation},
	 * this consumes that token from the input, leaving the reader
	 * ready for the next call to {@link #peekToken}.
	 *
	 * @param token must be the last token returned by {@link #peekToken}
	 */
	void consumeFixedToken(Token token);

	/**
	 * After {@link #peekToken} returns {@link Token#NUMBER NUMBER},
	 * this returns the character data
	 * comprising the text representation of the number.
	 * <p>
	 * Consumes the number from the input, leaving the reader
	 * ready for the next call to {@link #peekToken}.
	 */
	CharSequence consumeNumber();

	/**
	 * After {@link #peekToken} returns {@link Token#STRING STRING},
	 * this returns the characters comprising the string's value.
	 * Useful when there are opportunities to inspect only parts of the string's contents,
	 * such as when it's known to be an element of a fixed set of strings,
	 * leading to highly efficient parsing.
	 * <p>
	 * The string input is not actually consumed
	 * until the returned {@link JsonStringCharacterReader} itself is consumed,
	 * either by calling {@link JsonStringCharacterReader#skipToEnd() skipToEnd}
	 * or when {@link JsonStringCharacterReader#nextChar()} returns -1.
	 *
	 * @see #consumeStringContents()
	 */
	JsonStringCharacterReader consumeString();

	/**
	 * A variant of {@link #consumeString} that returns the entire string's contents as a {@link CharSequence}.
	 * Handy if you need the entire string.
	 * <p>
	 * Consumes the string input, leaving the reader
	 * ready for the next call to {@link #peekToken}.
	 *
	 * @see #consumeString()
	 */
	default CharSequence consumeStringContents() {
		JsonStringCharacterReader sr = consumeString();
		StringBuilder sb = new StringBuilder();
		int c;
		while ((c = sr.nextChar()) != -1) {
			sb.appendCodePoint(c);
		}
		return sb;
	}

	default String readString() {
		return consumeStringContents().toString();
	}

	@Override void close(); // No throws Exception

}
