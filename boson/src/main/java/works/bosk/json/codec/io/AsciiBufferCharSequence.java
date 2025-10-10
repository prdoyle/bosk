package works.bosk.json.codec.io;

import java.nio.ByteBuffer;

/**
 * A high-performance lightweight CharSequence implementation
 * that wraps a byte array containing only 7-bit ASCII characters.
 * <p>
 * If the buffer contains non-ASCII characters, weirdness will ensue.
 */
final class AsciiBufferCharSequence implements CharSequence {
	private final ByteBuffer buffer;
	private final int start;
	private final int length;

	AsciiBufferCharSequence(ByteBuffer buffer, int start, int length) {
		this.buffer = buffer;
		this.start = start;
		this.length = length;
	}

	@Override
	public int length() {
		return length;
	}

	@Override
	public char charAt(int index) {
		if (index < 0 || index >= length) {
			throw new IndexOutOfBoundsException();
		}
		return (char) buffer.get(start + index);
	}

	@Override
	public CharSequence subSequence(int start, int end) throws IndexOutOfBoundsException {
		if (start < 0 || end > length) {
			throw new IndexOutOfBoundsException();
		}
		return new AsciiBufferCharSequence(buffer, this.start + start, end - start);
	}

	@Override
	public String toString() {
		int pos = buffer.position();
		byte[] bytes = new byte[length];
		buffer.position(start);
		buffer.get(bytes);
		buffer.position(pos);
		return new String(bytes, java.nio.charset.StandardCharsets.US_ASCII);
	}
}
