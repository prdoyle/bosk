package works.bosk.boson.codec;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import works.bosk.boson.codec.io.Util;
import works.bosk.boson.exceptions.JsonLexicalException;
import works.bosk.boson.exceptions.JsonProcessingException;
import works.bosk.boson.exceptions.JsonSyntaxException;
import works.bosk.boson.exceptions.JsonValidityException;

import static java.util.stream.Collectors.toSet;
import static works.bosk.boson.codec.Token.END_ARRAY;
import static works.bosk.boson.codec.Token.END_OBJECT;
import static works.bosk.boson.codec.Token.END_TEXT;
import static works.bosk.boson.codec.Token.ERROR;
import static works.bosk.boson.codec.Token.FALSE;
import static works.bosk.boson.codec.Token.NULL;
import static works.bosk.boson.codec.Token.NUMBER;
import static works.bosk.boson.codec.Token.START_ARRAY;
import static works.bosk.boson.codec.Token.START_OBJECT;
import static works.bosk.boson.codec.Token.STRING;
import static works.bosk.boson.codec.Token.TRUE;

/**
 * Stackable layer that adds lexical and syntactical validation to a given {@link JsonReader}.
 * <p>
 * The {@link JsonReader} interface itself is vague as to how much validation it does,
 * specifying only a bare minimum.
 * This class ensures that the input is free of errors that would cause a
 * {@link JsonValidityException}.
 * Without this, a reader typically does only the validation required
 * by the {@link JsonReader} interface.
 * <p>
 * This isn't validating that the caller is using the reader correctly;
 * only that the input is valid JSON.
 *
 */
public final class ValidatingReader implements JsonReader {
	private final JsonReader downstream;
	private final Deque<Expect> stack = new ArrayDeque<>();

	public ValidatingReader(JsonReader downstream) {
		this.downstream = downstream;
		stack.push(Expect.VALUE);
	}

	private enum Expect {
		VALUE("value", valueStartingTokenOr()),
		ARRAY_ELEMENT("array element or closing bracket", valueStartingTokenOr(END_ARRAY)),
		MEMBER_NAME("object member name or closing brace", Set.of(STRING, END_OBJECT)),
		MEMBER_VALUE("value", valueStartingTokenOr()),
		END_OF_INPUT("end of input", Set.of(END_TEXT));

		final String description;
		final Set<Token> compatibleTokens;

		Expect(String description, Set<Token> compatibleTokens) {
			this.description = description;
			this.compatibleTokens = compatibleTokens;
		}

		private static Set<Token> valueStartingTokenOr(Token... others) {
			return Stream.concat(
				Stream.of(START_OBJECT, START_ARRAY, STRING, NUMBER, TRUE, FALSE, NULL),
				Stream.of(others)
			).collect(toSet());
		}
	}

	private Expect expected() {
		Expect result = stack.peek();
		assert result != null: "Stack should never be empty";
		return result;
	}

	/**
	 * Ensures that the given token is valid in the current context.
	 * If it's a {@code START_ARRAY} or {@code START_OBJECT},
	 * pushes the appropriate expectation onto the stack.
	 * If it's an {@code END_ARRAY} or {@code END_OBJECT},
	 * pops the stack and adjusts the expectation accordingly.
	 * @return boolean instead of throwing because
	 * the type of exception to throw depends on context
	 */
	private boolean checkAndAdjustStack(Token token) {
		Expect expected = expected();
		boolean result = expected.compatibleTokens.contains(token);
		if (result) {
			// Ok, it's an expected token. Adjust the stack.
			if (expected == Expect.MEMBER_NAME) {
				if (token == STRING) {
					// This is the name. Now we expect a value.
					stack.pop();
					stack.push(Expect.MEMBER_VALUE);
				} else {
					assert token == END_OBJECT;
					stack.pop();
					checkAndAdjustStackAfterValue();
				}
			} else {
				// In every other context, we're expecting a value
				assert expected != Expect.END_OF_INPUT: "Already handled above";
				switch (token) {
					case START_OBJECT ->
						stack.push(Expect.MEMBER_NAME);
					case START_ARRAY ->
						stack.push(Expect.ARRAY_ELEMENT);
					case END_ARRAY -> {
						stack.pop();
						checkAndAdjustStackAfterValue();
					}
					default -> checkAndAdjustStackAfterValue();
				}
			}
		} else {
			throw new JsonSyntaxException(
				"Unexpected token " + token +
					" at offset " + currentOffset() +
					"; expected " + expected.description);
		}
		return result;
	}

	private void checkAndAdjustStackAfterValue() {
		Expect next = AFTER_VALUE.get(expected());
		assert next != null:
			"checkAndAdjustStackAfterValue is valid only when a JSON value is expected";
		stack.pop();
		stack.push(next);
	}

	private static final Map<Expect, Expect> AFTER_VALUE = Map.of(
		Expect.VALUE, Expect.END_OF_INPUT,
		Expect.MEMBER_VALUE, Expect.MEMBER_NAME,
		Expect.ARRAY_ELEMENT, Expect.ARRAY_ELEMENT
	);

	@Override
	public void close() {
		if (downstream.peekToken() != END_TEXT) {
			throw new JsonProcessingException(
				"Reader closed before end of input. " +
					" at offset " + currentOffset());
		}
		if (!checkAndAdjustStack(downstream.peekToken())) {
			throw new JsonSyntaxException(
				"Unexpected end of input at offset " + currentOffset() +
					"; expected " + expected().description);
		}
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
		if (!checkAndAdjustStack(token)) {
			throw new JsonSyntaxException(
				"Unexpected token " + token +
					" at offset " + currentOffset() +
					"; expected " + expected().description);
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
		if (!Character.isDigit(seq.charAt(seq.length() - 1))) {
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
		// Make sure it's a valid JSON character or -1
		if (0 <= i && i < 0x20 || i == '"' || i == '\\') {
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
