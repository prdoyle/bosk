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
	private ByteChunk currentChunk;

	/**
	 * The number of bytes that appeared in the input text before
	 * the start of the current chunk.
	 */
	private long currentChunkStartOffset = 0;

	/**
	 * The current position within the current chunk.
	 * Always between 0 and currentChunk.length(), inclusive.
	 * If equal to currentChunk.length(), then the next byte
	 * to be read is in the next chunk (which may not exist).
	 */
	private int currentChunkPos = 0;

	public ByteChunkJsonReader(ChunkFiller chunkFiller) {
		this.filler = chunkFiller;
		// TODO: Not ideal. There's no reason to block here until we actually need data.
		this.currentChunk = this.filler.nextChunk();
	}

	@Override
	public Token peekToken() {
		skipInsignificant();
		return peekRawToken();
	}

	private Token peekRawToken() {
		while (currentChunk != null && currentChunkPos >= currentChunk.length()) {
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
		int limit = currentChunk.length();

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
			int limit = currentChunk.length();

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

			// Need another buffer
			if (!nextBuffer()) {
				// whoops, the input ended in the middle of a number
				return sb;
			}

			// Continue from the start of the new buffer
			pos = 0;
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
		int actualLength = min(requestedLength, currentChunk.length() - currentChunkPos - 1);
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

			int remaining = currentChunk.length() - currentChunkPos;
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
			int limit = currentChunk.length();
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

		currentChunkStartOffset += currentChunk.length();
		filler.recycleChunk(currentChunk);
		currentChunk = filler.nextChunk();
		currentChunkPos = 0;

		return currentChunk != null;
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
		while (currentChunk != null && currentChunkPos >= currentChunk.length()) {
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
			if (currentChunkPos >= currentChunk.length()) {
				nextBuffer();
			}
		}
	}

}
