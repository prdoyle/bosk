package works.bosk.json.codec.io;

import works.bosk.json.mapping.Token;

final class CharArrayJsonReader implements JsonReader {
	final char[] chars;
	int pos = 0;

	CharArrayJsonReader(char[] chars) {
		this.chars = chars;
	}

	@Override
	public Token peekToken() {
		skipInsignificant();
		return Token.startingWith(peekChar());
	}

	/**
	 * @return NOT a code point!
	 */
	private int peekChar() {
		if (pos >= chars.length) {
			return -1;
		} else {
			return chars[pos];
		}
	}

	private void skipInsignificant() {
		while (Util.fast_isInsignificant(peekChar())) {
			pos++;
		}
	}

	@Override
	public void consumeFixedToken(Token token) {
		assert peekToken() == token;
		pos += token.fixedRepresentation().length();
	}

	@Override
	public CharSequence consumeNumber() {
		int start = pos;
		while (pos < chars.length && Util.isNumberChar(chars[pos])) {
			pos++;
		}
		return new CharArraySequence(start, pos);
	}

	@Override
	public JsonStringCharacterReader processString() {
		pos++; // Skip opening quote
		return new StringCharacterReader();
	}

	@Override
	public void close() {

	}

	@Override
	public String consumeString() {
		if (false) {
			return JsonReader.super.consumeString();
		} else {
			// We can do better than the default implementation
			int start = ++pos;
			while (pos <= chars.length && chars[pos] != '"') {
				pos++;
			}
			try {
				return new String(chars, start, pos - start);
			} finally {
				pos++; // Skip closing quote
			}
		}
	}

	private class CharArraySequence implements CharSequence {
		private final int start;
		private final int stop;

		public CharArraySequence(int start, int stop) {
			this.start = start;
			this.stop = stop;
		}

		@Override
		public int length() {
			return stop - start;
		}

		@Override
		public char charAt(int index) {
			return chars[start + index];
		}

		@Override
		public CharSequence subSequence(int start, int end) {
			if (start < 0 || end > length() || start > end) {
				throw new IndexOutOfBoundsException();
			} else {
				return new CharArraySequence(this.start + start, this.start + end);
			}
		}

		@Override
		public String toString() {
			return new String(chars, start, length());
		}
	}

	final class StringCharacterReader implements JsonStringCharacterReader {
		@Override
		public int nextChar() {
			if (pos >= chars.length) {
				return -1;
			}
			char c = chars[pos++];
			if (Character.isSurrogate(c)) {
				if (pos >= chars.length) {
					// Unpaired surrogate at the end of input.
					// We're doomed to get a parse error; might
					// as well return -1 to indicate the end of input
					// in hopes that will generate a useful error message.
					return -1;
				}
				return Character.toCodePoint(c, chars[pos++]);
			} else if (c == '"') {
				return -1;
			} else {
				return c;
			}
		}

		@Override
		public void skipChars(int n) {
			for (int i = 0; i < n; i++) {
				int c = nextChar();
				if (c == -1) {
					throw new IllegalStateException("Attempt to skip past end of string");
				}
			}
		}

		@Override
		public void skipToEnd() {
			while (nextChar() != -1) { }
		}
	}
}
