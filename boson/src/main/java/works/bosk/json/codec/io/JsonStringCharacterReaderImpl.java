package works.bosk.json.codec.io;

/**
 * Implementation of JsonStringCharacterReader.
 */
final class JsonStringCharacterReaderImpl implements JsonStringCharacterReader {
	private final JsonReaderImpl reader;

	public JsonStringCharacterReaderImpl(JsonReaderImpl reader) {
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
		reader.advance();
		return b;
	}

	@Override
	public void skipChars(int n) {
		for (int i = 0; i < n; i++) {
			int c = nextChar();
			if (c == -1) {
				throw new IllegalStateException("Unexpected end of string while skipping characters");
			}
		}
	}

	@Override
	public void skipToEnd(int n) {
		skipChars(n);
		byte delimiter = reader.peekByte();
		assert delimiter == '"';
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
}
