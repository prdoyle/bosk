package works.bosk.json.codec.io;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import static works.bosk.json.codec.io.JsonReader.Token.BEGIN_ARRAY;
import static works.bosk.json.codec.io.JsonReader.Token.BEGIN_OBJECT;
import static works.bosk.json.codec.io.JsonReader.Token.END_ARRAY;
import static works.bosk.json.codec.io.JsonReader.Token.END_DOCUMENT;
import static works.bosk.json.codec.io.JsonReader.Token.END_OBJECT;
import static works.bosk.json.codec.io.JsonReader.Token.FALSE;
import static works.bosk.json.codec.io.JsonReader.Token.NULL;
import static works.bosk.json.codec.io.JsonReader.Token.NUMBER;
import static works.bosk.json.codec.io.JsonReader.Token.STRING;
import static works.bosk.json.codec.io.JsonReader.Token.TRUE;

/**
 * {@link JsonReader} that uses an OverlappedPrefetcher for high-throughput buffer management.
 */
public final class JsonReaderImpl implements JsonReader {
	private final OverlappedPrefetcher prefetcher;
	private ByteBuffer buffer;

	public JsonReaderImpl(ReadableByteChannel channel) {
		this.prefetcher = new OverlappedPrefetcher(channel);
		this.buffer = this.prefetcher.nextBuffer();
	}

	@Override
	public Token nextToken() {
		skipInsignificant();
		if (buffer == null) {
			return END_DOCUMENT;
		}

		byte b = peekByte();

		switch (b) {
			case '{': { advance(); return BEGIN_OBJECT; }
			case '}': { advance(); return END_OBJECT; }
			case '[': { advance(); return BEGIN_ARRAY; }
			case ']': { advance(); return END_ARRAY; }
			case '"': { advance(); return STRING; }
			case 't': {
				if (matchLiteral("true")) {
					return TRUE;
				}
				break;
			}
			case 'f': {
				if (matchLiteral("false")) {
					return FALSE;
				}
				break;
			}
			case 'n': {
				if (matchLiteral("null")) {
					return NULL;
				}
				break;
			}
			default: {
				if (b == '-' || (b >= '0' && b <= '9')) {
					return NUMBER;
				}
				break;
			}
		}

		throw new IllegalStateException("Unexpected JSON token: " + (char) b);
	}

	@Override
	public CharSequence numberChars() {
		int startPos = buffer.position();
		var startBuffer = buffer;

		while (true) {
			if (!buffer.hasRemaining()) {
				// We've run out of buffer
				return numberStringBuilder(startPos);
			}
			byte b = peekByte();
			if (isNumberChar(b)) {
				advance();
			} else {
				// End of the number
				assert startBuffer == buffer;
				return new AsciiBufferCharSequence(buffer, startPos, buffer.position()-startPos);
			}
		}
	}

	@Override
	public JsonStringCharacterReader stringCharacterReader() {
		return new JsonStringCharacterReaderImpl(this);
	}

	/**
	 * A generalized (if slow) way to build a {@link CharSequence} for a number
	 * regardless of whether it spans buffer boundaries or contains non-ASCII characters.
	 */
	private CharSequence numberStringBuilder(int startPos) {
		StringBuilder sb = new StringBuilder();
	
		// Back up to the start of the number
		buffer.position(startPos);

		for (byte b = peekByte(); isNumberChar(b); b = peekByte()) {
			sb.append((char) b);
			advance();
		}
		
		return sb;
	}

	private boolean isNumberChar(byte b) {
		return (b >= '0' && b <= '9') || b == '.' || b == '-' || b == '+' || b == 'e' || b == 'E';
	}

	private boolean matchLiteral(String literal) {
		for (int i = 0; i < literal.length(); i++) {
			if (peekByte() != literal.charAt(i)) {
				return false;
			}
			advance();
		}
		return true;
	}

	byte peekByte() {
		if (ensureRemaining()) {
			return buffer.get(buffer.position());
		} else {
			return -1;
		}
	}

	void advance() {
		if (ensureRemaining()) {
			buffer.get();
		}
	}

	private boolean ensureRemaining() {
		while (!buffer.hasRemaining()) {
			buffer = prefetcher.nextBuffer();
			if (buffer == null) {
				return false;
			}
		}
		return true;
	}

	private void skipInsignificant() {
		loop: while (true) {
			byte b = peekByte();
			switch (b) {
				case ' ', '\n', '\r', '\t', ':', ',' -> advance();
				default -> { break loop; }
			}
		}
	}
}
