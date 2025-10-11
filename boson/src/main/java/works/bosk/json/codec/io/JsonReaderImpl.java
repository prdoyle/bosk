package works.bosk.json.codec.io;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import works.bosk.json.mapping.Token;

import static works.bosk.json.mapping.Token.END_TEXT;
import static works.bosk.json.mapping.Token.NUMBER;
import static works.bosk.json.mapping.Token.STRING;


/**
 * {@link JsonReader} that uses an {@link OverlappedPrefetcher} to read from a {@link ReadableByteChannel}.
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
	public Token peekToken() {
		skipInsignificant();
		return peekRawToken();
	}

	/**
	 * May return {@link Token#INSIGNIFICANT}.
	 */
	private Token peekRawToken() {
		if (currentBuf == null) {
			return END_TEXT;
		} else {
			return Token.startingWith(peekByte());
		}
	}

	@Override
	public void consumeFixedToken(Token token) {
		assert peekRawToken() == token;
		assert token.hasFixedRepresentation();
		skip(token.fixedRepresentation().length());
	}

	@Override
	public CharSequence consumeNumber() {
		assert peekRawToken() == NUMBER;
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
	public JsonStringCharacterReader consumeString() {
		assert peekRawToken() == STRING;
		skip(1); // Opening quote
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
	
	void skip(int n) {
		if (n == 0) {
			// This case needs to work even if currentBuf is null
			return;
		}
		if (n < 0) {
			throw new IllegalArgumentException("Can't skip a negative number of bytes: " + n);
		}
		int remaining;
		while (n >= (remaining = currentBuf.remaining())) {
			n -= remaining;
			if (!nextBuffer()) {
				return;
			}
		}
		currentBuf.position(currentBuf.position() + n);
	}

	private boolean hasRemaining() {
		if (currentBuf == null) {
			return false;
		}
		while (!currentBuf.hasRemaining()) {
			if (!nextBuffer()) {
				return false;
			}
		}
		return true;
	}

	private boolean nextBuffer() {
		prefetcher.recycleBuffer(currentBuf);
		currentBuf = prefetcher.nextBuffer();
		return currentBuf != null;
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
