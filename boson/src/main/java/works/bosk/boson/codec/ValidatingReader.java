package works.bosk.boson.codec;

import works.bosk.boson.codec.io.Util;
import works.bosk.boson.exceptions.JsonLexicalException;
import works.bosk.boson.exceptions.JsonSyntaxException;
import works.bosk.boson.exceptions.JsonValidityException;

import static works.bosk.boson.codec.Token.ERROR;

/**
 * Stackable layer that adds lexical and syntactical validation to a given {@link JsonReader}.
 * <p>
 * The {@link JsonReader} interface itself is vague as to how much validation it does,
 * specifying only a bare minimum.
 * This class ensures that the input is free of errors that would cause a
 * {@link JsonValidityException}.
 * Without this, a reader typically does only the validation required
 * by the {@link JsonReader} interface.
 *
 */
public record ValidatingReader(JsonReader downstream) implements JsonReader {
	@Override
	public void close() {
		downstream.close();
	}

	@Override
	public Token peekToken() {
		Token result = downstream.peekToken();
		if (result == ERROR) {
			throw new JsonLexicalException("Invalid JSON syntax at offset " + currentOffset());
		}
		return result;
	}

	@Override
	public void consumeFixedToken(Token token) {
		if (!token.equals(peekToken())) {
			throw new JsonSyntaxException(
				"Expected token " + token + " but found " + peekToken() +
				" at offset " + currentOffset());
		}
		downstream.validateCharacters(token.fixedRepresentation());
	}

	@Override
	public CharSequence consumeNumber() {
		CharSequence result = downstream.consumeNumber();
		if (!isValidJsonNumber(result)) {
			throw new JsonLexicalException(
				"Invalid JSON number '" + result + "' at offset " + currentOffset());
		}
		return result;
	}

	private boolean isValidJsonNumber(CharSequence seq) {
		if ("0".contentEquals(seq)) {
			return true;
		}
		if (!Util.isNumberLeadingChar(seq.charAt(0))) {
			throw new JsonLexicalException(
				"Invalid leading character in JSON number: '" + seq + "'"
			);
		}
		if (!Character.isDigit(seq.charAt(seq.length()-1))) {
			throw new JsonLexicalException(
				"Invalid trailing character in JSON number: '" + seq + "'"
			);
		}
		try {
			Double.parseDouble(seq.toString());
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	@Override
	public void startConsumingString() {
		downstream.startConsumingString();
	}

	@Override
	public int nextStringChar() {
		int i = downstream.nextStringChar();
		// Make sure it's a valid JSON character
		if (i < 0x20 || i == '"' || i == '\\') {
			throw new JsonLexicalException(
				"Invalid character " + Character.getName(i) + " in JSON string at offset " + currentOffset()
			);
		}
		return i;
	}

	@Override
	public void skipStringChars(int n) {
		downstream.skipStringChars(n);
	}

	@Override
	public void skipToEndOfString() {
		downstream.skipToEndOfString();
	}

	@Override
	public void validateCharacters(CharSequence expectedCharacters) {
		downstream.validateCharacters(expectedCharacters);
	}

	@Override
	public String previewString(int requestedLength) {
		return downstream.previewString(requestedLength);
	}

	@Override
	public long currentOffset() {
		return downstream.currentOffset();
	}
}
