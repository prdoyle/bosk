package works.bosk.json.codec.io;

/**
 * Implementation of JsonStringCharacterReader.
 */
final class JsonStringCharacterReaderImpl implements JsonStringCharacterReader {
	private final ByteBufferJsonReader reader;

	public JsonStringCharacterReaderImpl(ByteBufferJsonReader reader) {
		this.reader = reader;
	}

	@Override
	public int nextChar() {
		int b = reader.peekByte();
		switch (b) {
			case -1 -> {
				throw new IllegalStateException("Unexpected end of string before closing quote");
			}
			case '"' -> {
				reader.advance(); // Eat the quote
				return -1;
			}
			case '\\' -> {
				reader.advance();
				int esc = reader.peekByte();
				if (esc == -1) {
					throw new IllegalStateException("Unexpected end of string after escape backslash");
				}
				reader.advance();
				if (esc == 'u') {
					return decodeUnicodeEscape();
				}
				return decodeEscapeChar(esc);
			}
		}

		// Normal character
		if ((b & 0x80) == 0) {
			// ASCII fast path
			reader.advance();
			return b;
		} else {
			// Decode UTF-8 multibyte sequence
			return decodeUtf8Char();
		}
	}

	@Override
	public void skipChars(int n) {
		if (n < 0) {
			throw new IllegalArgumentException("Must skip a non-negative number of characters, got " + n);
		}
		for (int i = n; i > 0; --i) {
			int c = nextChar();
			if (c == -1) {
				if (i != 1) {
					throw new IllegalStateException("Unexpected end of string while skipping characters");
				}
			}
		}
	}

	@Override
	public void skipToEnd() {
		while (nextChar() != -1) {}
	}

	private int decodeUnicodeEscape() {
		int value = 0;
		for (int i = 0; i < 4; i++) {
			int b = reader.peekByte();
			reader.advance();
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
		int b1 = reader.peekByte();
		reader.advance();

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
			int bx = reader.peekByte();
			reader.advance();
			if ((bx & 0xC0) != 0x80) {
				throw new IllegalStateException("Invalid UTF-8 continuation byte: " + bx);
			}
			codePoint = (codePoint << 6) | (bx & 0x3F);
		}
		return codePoint;
	}
}
