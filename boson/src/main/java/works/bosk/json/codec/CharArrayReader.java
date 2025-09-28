package works.bosk.json.codec;

public class CharArrayReader {
	private final char[] chars;
	private int offset;

	public CharArrayReader(String json) {
		this(json.toCharArray(), 0);
	}

	public CharArrayReader(char[] chars, int offset) {
		this.chars = chars;
		this.offset = offset;
	}

	public int offset() {
		return offset;
	}

	public int read() {
		try {
			int result = chars[offset];
			++offset;
			return result;
		} catch (ArrayIndexOutOfBoundsException e) {
			return -1;
		}
	}

	public String previewString(int length) {
		length = Math.min(length, chars.length - offset);
		return new String(chars, offset, length);
	}

	public int peek() {
		if (offset >= chars.length) {
			return -1; // End of stream
		}
		return chars[offset];
	}

	public void seek(int newOffset) {
		if (newOffset < 0 || newOffset >= chars.length) {
			throw new IndexOutOfBoundsException("Cannot seek to offset " + newOffset + " in char array of length " + chars.length);
		}
		this.offset = newOffset;
	}

	public void skip(int n) {
		if (n < -offset || offset + n > chars.length) {
			throw new IndexOutOfBoundsException("Cannot skip " + n + " characters from offset " + offset);
		}
		offset += n;
	}
}
