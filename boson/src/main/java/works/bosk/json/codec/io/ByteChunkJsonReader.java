package works.bosk.json.codec.io;

import works.bosk.json.codec.JsonReader;
import works.bosk.json.codec.JsonStringCharacterReader;
import works.bosk.json.mapping.Token;

import static java.lang.Math.min;
import static works.bosk.json.mapping.Token.END_TEXT;
import static works.bosk.json.mapping.Token.NUMBER;
import static works.bosk.json.mapping.Token.STRING;


/**
 * {@link JsonReader} that uses a {@link ChunkFiller}.
 * <p>
 * Calling {@link #close()} will close the underlying channel.
 */
public final class ByteChunkJsonReader implements JsonReader {
	private final ChunkFiller filler;
	private ByteChunk currentBuf;

	/**
	 * The offset {@code currentBuf} in the overall byte stream.
	 */
	private long currentBufStartOffset = 0;

	/**
	 * Current position within {@code currentBuf}.
	 */
	private int currentBufPos = 0;

	public ByteChunkJsonReader(ChunkFiller chunkFiller) {
		this.filler = chunkFiller;
		// TODO: Not ideal. There's no reason to block here until we actually need data.
		this.currentBuf = this.filler.nextChunk();
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
		int startPos = currentBufPos;
		var startBuffer = currentBuf;

		while (true) {
			if (currentBufPos == currentBuf.length()) {
				// We've run out of buffer
				return numberStringBuilder(startPos);
			}
			byte b = peekByte();
			if (Util.isNumberChar(b)) {
				advance();
			} else {
				// End of the number
				assert startBuffer == currentBuf;
				return new AsciiChunkCharSequence(currentBuf, startPos, currentBufPos-startPos);
			}
		}
	}

	@Override
	public JsonStringCharacterReader processString() {
		assert peekRawToken() == STRING;
		skip(1); // Opening quote
		return new JsonStringCharacterReaderImpl(this);
	}

	@Override
	public String previewString(int requestedLength) {
		int actualLength = min(requestedLength, currentBuf.length() - currentBufPos - 1);
		StringBuilder sb = new StringBuilder(actualLength);
		for (int i = 0; i < actualLength; i++) {
			byte b = currentBuf.bytes()[currentBufPos + i];
			sb.append((char) b); // Only works for ASCII
		}
		return sb.toString();
	}

	/**
	 * A generalized (if slow) way to build a {@link CharSequence} for a number
	 * regardless of whether it spans buffer boundaries or contains non-ASCII characters.
	 */
	private CharSequence numberStringBuilder(int startPos) {
		StringBuilder sb = new StringBuilder();
	
		// Back up to the start of the number
		currentBufPos = startPos;

		for (byte b = peekByte(); Util.isNumberChar(b); b = peekByte()) {
			sb.append((char) b);
			advance();
		}
		
		return sb;
	}

	byte peekByte() {
		if (hasRemaining()) {
			return currentBuf.bytes()[currentBufPos];
		} else {
			return -1;
		}
	}

	void advance() {
		if (hasRemaining()) {
			currentBufPos++;
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
		while (n >= (remaining = currentBuf.length() - currentBufPos)) {
			n -= remaining;
			if (!nextBuffer()) {
				return;
			}
		}
		currentBufPos += n;
	}

	private boolean hasRemaining() {
		if (currentBuf == null) {
			return false;
		}
		while (currentBufPos >= currentBuf.length()) {
			if (!nextBuffer()) {
				return false;
			}
		}
		return true;
	}

	private boolean nextBuffer() {
		assert currentBuf != null;
		currentBufStartOffset += currentBuf.length();
		filler.recycleChunk(currentBuf);
		currentBuf = filler.nextChunk();
		currentBufPos = 0;
		return currentBuf != null;
	}

	private void skipInsignificant() {
		while (Util.fast_isInsignificant(peekByte())) {
			advance();
		}
	}

	@Override
	public void close() {
		filler.close();
	}

	@Override
	public long currentOffset() {
		return currentBufStartOffset + (currentBuf == null ? 0 : currentBufPos);
	}
}
