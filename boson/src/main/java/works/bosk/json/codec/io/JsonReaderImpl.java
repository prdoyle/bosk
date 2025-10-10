package works.bosk.json.codec.io;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import works.bosk.json.mapping.Token;

import static works.bosk.json.mapping.Token.END_ARRAY;
import static works.bosk.json.mapping.Token.END_OBJECT;
import static works.bosk.json.mapping.Token.END_TEXT;
import static works.bosk.json.mapping.Token.FALSE;
import static works.bosk.json.mapping.Token.NULL;
import static works.bosk.json.mapping.Token.NUMBER;
import static works.bosk.json.mapping.Token.START_ARRAY;
import static works.bosk.json.mapping.Token.START_OBJECT;
import static works.bosk.json.mapping.Token.STRING;
import static works.bosk.json.mapping.Token.TRUE;


/**
 * {@link JsonReader} that uses an {@link OverlappedPrefetcher} for high-throughput buffer management.
 * <p>
 * Calling {@link #close()} will close the underlying channel.
 */
final class JsonReaderImpl implements JsonReader {
	private final OverlappedPrefetcher prefetcher;
	private ByteBuffer currentBuf;

	public JsonReaderImpl(ReadableByteChannel channel) {
		this.prefetcher = new OverlappedPrefetcher(channel);
		// TODO: Not ideal. There's no reason to block here until we actually need data.
		this.currentBuf = this.prefetcher.nextBuffer();
	}

	@Override
	public Token nextToken() {
		skipInsignificant();
		if (currentBuf == null) {
			return END_TEXT;
		}

		byte b = peekByte();

		switch (b) {
			case '{': { advance(); return START_OBJECT; }
			case '}': { advance(); return END_OBJECT; }
			case '[': { advance(); return START_ARRAY; }
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
		int startPos = currentBuf.position();
		var startBuffer = currentBuf;

		while (true) {
			if (!currentBuf.hasRemaining()) {
				// We've run out of buffer
				return numberStringBuilder(startPos);
			}
			byte b = peekByte();
			if (isNumberChar(b)) {
				advance();
			} else {
				// End of the number
				assert startBuffer == currentBuf;
				return new AsciiBufferCharSequence(currentBuf, startPos, currentBuf.position()-startPos);
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
		currentBuf.position(startPos);

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
		if (hasRemaining()) {
			return currentBuf.get(currentBuf.position());
		} else {
			return -1;
		}
	}

	void advance() {
		if (hasRemaining()) {
			currentBuf.get();
		}
	}

	private boolean hasRemaining() {
		while (!currentBuf.hasRemaining()) {
			currentBuf = prefetcher.nextBuffer();
			if (currentBuf == null) {
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

	@Override
	public void close() {
		prefetcher.close();
	}
}
