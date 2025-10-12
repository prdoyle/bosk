package works.bosk.json.codec.io;

import works.bosk.json.codec.JsonReader;
import works.bosk.json.mapping.Token;

import static java.lang.Math.min;
import static works.bosk.json.mapping.Token.END_TEXT;
import static works.bosk.json.mapping.Token.NUMBER;


/**
 * {@link JsonReader} that uses a {@link ChunkFiller}.
 * <p>
 * Calling {@link #close()} will close the underlying channel.
 */
public final class ByteChunkJsonReader implements JsonReader {
	/**
	 * The number of bytes that must be carried over from one chunk to the next
	 * to ensure that we can always parse a JSON string character that crosses a chunk boundary.
	 * The largest JSON string character is an escaped 4-byte UTF-8 character,
	 * which takes 6 bytes total (backslash, 'u', and 4 hex digits),
	 * so we need to carry over at most 5 bytes.
	 */
	static final int CARRYOVER_BYTES = 5;

	private final ChunkFiller filler;
	private ByteChunk currentChunk;

	/**
	 * The number of bytes that appeared in the input text before
	 * the start of the current chunk.
	 */
	private long currentChunkStartOffset = 0;

	/**
	 * The current index within the current chunk.
	 * Always between currentChunk.start() and currentChunk.stop(), inclusive.
	 * If equal to currentChunk.stop(), then the next byte
	 * to be read is in the next chunk (which may not exist).
	 */
	private int currentChunkPos;

	public ByteChunkJsonReader(ChunkFiller chunkFiller) {
		this.filler = chunkFiller;
		// TODO: Not ideal. There's no reason to block here until we actually need data.
		this.currentChunk = this.filler.nextChunk();
		this.currentChunkPos = currentChunk.start();
	}

	@Override
	public Token peekToken() {
		skipInsignificant();
		return peekRawToken();
	}

	private Token peekRawToken() {
		while (currentChunk != null && currentChunkPos >= currentChunk.stop()) {
			nextBuffer();
		}

		if (currentChunk == null) {
			return END_TEXT;
		} else {
			return Token.startingWith(currentChunk.bytes()[currentChunkPos]);
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

		int startPos = currentChunkPos;
		byte[] buf = currentChunk.bytes();
		int limit = currentChunk.stop();

		while (currentChunkPos < limit) {
			byte b = buf[currentChunkPos];
			if (!Util.isNumberChar(b)) {
				// We've reached the end of the number
				return new AsciiChunkCharSequence(currentChunk, startPos, currentChunkPos - startPos);
			} else {
				currentChunkPos++;
			}
		}

		// The number crosses a buffer boundary
		return numberStringBuilder(startPos);
	}

	@Override
	public void startConsumingString() {
		assert peekRawToken() == Token.STRING;
		advance(); // Eat the opening quote
	}

	@Override
	public int nextStringChar() {
		int b = peekByte();
		switch (b) {
			case -1 -> {
				throw new IllegalStateException("Unexpected end of string before closing quote");
			}
			case '"' -> {
				advance(); // Eat the quote
				return -1;
			}
			case '\\' -> {
				advance();
				int esc = peekByte();
				if (esc == -1) {
					throw new IllegalStateException("Unexpected end of string after escape backslash");
				}
				advance();
				if (esc == 'u') {
					return decodeUnicodeEscape();
				}
				return decodeEscapeChar(esc);
			}
		}

		// Normal character
		if ((b & 0x80) == 0) {
			// ASCII fast path
			advance();
			return b;
		} else {
			// Decode UTF-8 multibyte sequence
			return decodeUtf8Char();
		}
	}

	@Override
	public void skipStringChars(int n) {
		if (n < 0) {
			throw new IllegalArgumentException("Must skip a non-negative number of characters, got " + n);
		}
		for (int i = n; i > 0; --i) {
			int c = nextStringChar();
			if (c == -1) {
				if (i != 1) {
					throw new IllegalStateException("Unexpected end of string while skipping characters");
				}
			}
		}
	}

	@Override
	public void skipToEndOfString() {
		while (nextStringChar() != -1) {}
	}

	/**
	 * A generalized (if slow) way to build a {@link CharSequence} for a number
	 * regardless of whether it spans buffer boundaries or contains non-ASCII characters.
	 */
	private CharSequence numberStringBuilder(int startPos) {
		StringBuilder sb = new StringBuilder();

		// Rather than continually update the object field,
		// do most of the work on this local variable for speed.
		int pos = startPos;

		while (true) {
			byte[] buf = currentChunk.bytes();
			int limit = currentChunk.stop();

			while (pos < limit) {
				byte b = buf[pos];
				if (Util.isNumberChar(b)) {
					sb.append((char) b);
					pos++;
				} else {
					// Number ended
					currentChunkPos = pos;
					return sb;
				}
			}

			if (nextBuffer()) {
				// Continue from the start of the new buffer
				pos = currentChunk.start();
			} else {
				// whoops, the input ended in the middle of a number
				return sb;
			}
		}
	}

	@Override
	public String previewString(int requestedLength) {
		int actualLength = min(requestedLength, currentChunk.stop() - currentChunkPos - 1);
		char[] result = new char[actualLength];
		byte[] buf = currentChunk.bytes();
		for (int i = 0; i < actualLength; i++) {
			// TODO: handle non-ascii characters
			result[i] = (char) buf[currentChunkPos + i];
		}
		return new String(result);
	}

	void skip(int n) {
		if (n == 0) {
			return;
		}
		if (n < 0) {
			throw new IllegalArgumentException("Can't skip a negative number of bytes: " + n);
		}

		while (n > 0) {
			if (currentChunk == null) {
				return;
			}

			int remaining = currentChunk.stop() - currentChunkPos;
			if (n < remaining) {
				currentChunkPos += n;
				return;
			} else {
				n -= remaining;
				if (!nextBuffer()) {
					return;
				}
			}
		}
	}

	private void skipInsignificant() {
		while (currentChunk != null) {
			byte[] buf = currentChunk.bytes();
			int limit = currentChunk.stop();
			while (currentChunkPos < limit) {
				if (!Util.fast_isInsignificant(buf[currentChunkPos])) {
					return;
				} else {
					currentChunkPos++;
				}
			}

			if (!nextBuffer()) {
				return;
			}
		}
	}

	private boolean nextBuffer() {
		if (currentChunk == null) {
			return false;
		}

		currentChunkStartOffset += currentChunk.stop();
		filler.recycleChunk(currentChunk);
		currentChunk = filler.nextChunk();

		if (currentChunk == null) {
			return false;
		} else {
			currentChunkPos = currentChunk.start();
			return true;
		}
	}

	@Override
	public void close() {
		filler.close();
	}

	@Override
	public long currentOffset() {
		if (currentChunk != null) {
			return currentChunkStartOffset + currentChunkPos;
		} else {
			return currentChunkStartOffset;
		}
	}

	// numberStringBuilder() only called for multi-buffer numbers

	/**
	 * Relatively slow way to peek at the next byte, loading a new buffer if needed.
	 * Does not advance the position within the input text.
	 */
	byte peekByte() {
		while (currentChunk != null && currentChunkPos >= currentChunk.stop()) {
			if (!nextBuffer()) {
				return -1;
			}
		}

		if (currentChunk == null) {
			return -1;
		} else {
			return currentChunk.bytes()[currentChunkPos];
		}
	}

	/**
	 * Relatively slow way to advance one byte, loading a new buffer if needed.
	 */
	void advance() {
		if (currentChunk != null) {
			currentChunkPos++;
			if (currentChunkPos >= currentChunk.stop()) {
				nextBuffer();
			}
		}
	}

	private int decodeUnicodeEscape() {
		int value = 0;
		for (int i = 0; i < 4; i++) {
			int b = peekByte();
			advance();
			value <<= 4;
			value |= Character.digit(b, 16);
		}
		return value;
	}

	private int decodeEscapeChar(int b) {
		return switch (b) {
			case '"' -> '"';
			case '\\' -> '\\';
			case '/' -> '/';
			case 'b' -> '\b';
			case 'f' -> '\f';
			case 'n' -> '\n';
			case 'r' -> '\r';
			case 't' -> '\t';
			default -> throw new IllegalStateException("Invalid escape: \\" + (char) b);
		};
	}

	private int decodeUtf8Char() {
		// First byte tells us what we're dealing with
		int b1 = peekByte();
		advance();

		int codePoint;
		int sequenceLength;
		if ((b1 & 0xE0) == 0xC0) {
			sequenceLength = 2;
			codePoint = b1 & 0x1F;
		} else if ((b1 & 0xF0) == 0xE0) {
			sequenceLength = 3;
			codePoint = b1 & 0x0F;
		} else if ((b1 & 0xF8) == 0xF0) {
			sequenceLength = 4;
			codePoint = b1 & 0x07;
		} else {
			throw new IllegalStateException("Invalid UTF-8 start byte: " + b1);
		}

		for (int i = 1; i < sequenceLength; i++) {
			int bx = peekByte();
			advance();
			if ((bx & 0xC0) != 0x80) {
				throw new IllegalStateException("Invalid UTF-8 continuation byte: " + bx);
			}
			codePoint = (codePoint << 6) | (bx & 0x3F);
		}
		return codePoint;
	}
}
