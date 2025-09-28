package works.bosk.json;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import works.bosk.json.TestUtils.Month;
import works.bosk.json.codec.CharArrayReader;
import works.bosk.json.mapping.Token;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static works.bosk.json.TestUtils.ABSENT_FIELD_VALUE;
import static works.bosk.json.TestUtils.COMPUTED_FIELD_VALUE;
import static works.bosk.json.mapping.Token.END_OBJECT;
import static works.bosk.json.mapping.Token.FALSE;
import static works.bosk.json.mapping.Token.INSIGNIFICANT;
import static works.bosk.json.mapping.Token.NULL;
import static works.bosk.json.mapping.Token.NUMBER;
import static works.bosk.json.mapping.Token.START_OBJECT;
import static works.bosk.json.mapping.Token.STRING;
import static works.bosk.json.mapping.Token.TRUE;

public class ManualTest {
	/**
	 * Needs pushback only so we can read numbers!
	 */
	private CharArrayReader input;

	@BeforeEach
	void init() {
		input = new CharArrayReader(TestUtils.ONE_OF_EACH, 0);
	}

	private int read() throws IOException {
		return input.read();
	}

	private void skip(int n) throws IOException {
		input.skip(n);
	}

	@Test
	void oneOfEach() throws IOException {
		var expected = TestUtils.expectedOneOfEach();
		var actual = parse();
		assertEquals(expected, actual);
	}

	public TestUtils.OneOfEach parse() throws IOException {
		// Initialize these with all incorrect values
		String nullField = "whoops";
		Boolean trueField = null;
		Boolean falseField = null;
		Long integerField = null;
		Double realField = null;
		String stringField = null;
		List<String> stringArrayField = null;
		Map<TimeUnit, BigDecimal> mapField = null;
		Month monthField = null;
		String computedField = COMPUTED_FIELD_VALUE;
		String maybeAbsentField = ABSENT_FIELD_VALUE;

		expect(START_OBJECT);
		loop: while (true) {
			Token token = nextToken();
			switch (token) {
				case STRING -> { // member name
					switch (read()) {
						case 'n' -> nullField = (String) readAnyValue(firstMemberValueChar());
						case 't' -> trueField = (Boolean) readAnyValue(firstMemberValueChar());
						case 'f' -> falseField = (Boolean) readAnyValue(firstMemberValueChar());
						case 'i' -> integerField = readInteger(firstMemberValueChar());
						case 'r' -> realField = readDecimal(firstMemberValueChar());
						case 'm' -> {
							switch (read()) {
								case 'a' -> {
									switch (read()) {
										case 'p' -> mapField = readTimeUnitToBigDecimalMap(firstMemberValueChar());
										case 'y' -> maybeAbsentField = readString(firstMemberValueChar());
									}
								}
								case 'o' -> monthField = Month.fromValue((int)readInteger(firstMemberValueChar()));
							}
						}
						case 's' -> {
							skip(5);
							switch (read()) {
								case 'F' -> {
									// stringField
									stringField = readString(firstMemberValueChar());
								}
								case 'A' -> {
									// stringArrayField
									stringArrayField = readStringList(firstMemberValueChar());
								}
								default -> {
									throw new IllegalStateException("Parse error");
								}
							}
						}
						default -> {
							throw new IllegalStateException("Parse error");
						}
					}
				}
				case END_OBJECT -> {
					break loop;
				}
				default -> {
					throw new IllegalStateException("Unexpected token " + token);
				}
			}
		}
		skipToken(END_OBJECT);
		return new TestUtils.OneOfEach(
			nullField,
			trueField,
			falseField,
			integerField,
			realField,
			stringField,
			stringArrayField,
			mapField,
			monthField,
			computedField,
			maybeAbsentField);
	}

	private Map<TimeUnit, BigDecimal> readTimeUnitToBigDecimalMap(int firstChar) throws IOException {
		assert Token.startingWith(firstChar) == Token.START_OBJECT;
		Map<TimeUnit, BigDecimal> result = new java.util.LinkedHashMap<>();
		int c;
		while (Token.startingWith(c = nextSignificant()) != END_OBJECT) {
			var member = readString(c);
			var value = readBigNumber(nextSignificant());
			result.put(TimeUnit.valueOf(member), (BigDecimal) value);
		}
		return result;
	}

	private List<String> readStringList(int firstChar) throws IOException {
		assert Token.startingWith(firstChar) == Token.START_ARRAY;
		List<String> result = new java.util.ArrayList<>();
		int c;
		while (Token.startingWith(c = nextSignificant()) != Token.END_ARRAY) {
			result.add(readString(c));
		}
		return result;
	}

	private List<Object> readAnyList(int firstChar) throws IOException {
		assert Token.startingWith(firstChar) == Token.START_ARRAY;
		List<Object> result = new java.util.ArrayList<>();
		int c;
		while (Token.startingWith(c = nextSignificant()) != Token.END_ARRAY) {
			result.add(readAnyValue(c));
		}
		return result;
	}

	private Map<String, Object> readAnyMap(int firstChar) throws IOException {
		assert Token.startingWith(firstChar) == Token.START_OBJECT;
		Map<String, Object> result = new java.util.LinkedHashMap<>();
		int c;
		while (Token.startingWith(c = nextSignificant()) != END_OBJECT) {
			var member = readString(c);
			var value = readAnyValue(nextSignificant());
			result.put(member, value);
		}
		return result;
	}

	/**
	 * When positioned at either the start of a token or an {@link Token#INSIGNIFICANT},
	 * advance to the next character that is not insignificant and return its token.
	 */
	private Token nextToken() throws IOException {
		Token result;
		do {
			int read = read();
			result = Token.startingWith(read);
		} while (result == INSIGNIFICANT);
		return result;
	}

	private int firstMemberValueChar() throws IOException {
		eatMemberName();
		return nextSignificant();
	}

	private int nextSignificant() throws IOException {
		int result;
		do {
			result = read();
		} while (Token.startingWith(result) == INSIGNIFICANT);
		return result;
	}

	/**
	 * Skips the rest of the string.
	 */
	protected void eatMemberName() throws IOException {
		int c;
		while ((c= read()) != '"') {
			if (c == '\\') {
				input.skip(1);
			}
		}
	}

	private void skipToken(Token readToken) throws IOException {
		input.skip(readToken.fixedRepresentation().length() - 1);
	}

	private void expect(Token expectedToken) throws IOException {
		Token readToken = nextToken();
		if (readToken == expectedToken) {
			skipToken(readToken);
		} else {
			throw new IllegalStateException("Unexpected token " + readToken + "; expected " + expectedToken);
		}
	}

	private Object readAnyValue(int firstChar) throws IOException {
		switch (Token.startingWith(firstChar)) {
			case NULL -> {
				skipToken(NULL);
				return null;
			}
			case FALSE -> {
				skipToken(FALSE);
				return Boolean.FALSE;
			}
			case TRUE -> {
				skipToken(TRUE);
				return Boolean.TRUE;
			}
			case NUMBER -> {
				return readBigNumber(firstChar);
			}
			case START_OBJECT -> {
				return readAnyMap(firstChar);
			}
			case START_ARRAY -> {
				return readAnyList(firstChar);
			}
			case STRING -> {
				return readString(nextSignificant());
			}
			default -> {
				throw new IllegalStateException();
			}
		}
	}

	private String readString(int firstChar) throws IOException {
		assert Token.startingWith(firstChar) == STRING;
		StringBuilder sb = new StringBuilder();
		int c;
		while ((c = read()) != '"') {
			if (c == '\\') {
				sb.append((char)read());
			} else {
				sb.append((char)c);
			}
		}
		return sb.toString();
	}

	private long readInteger(int firstChar) throws IOException {
		CharSequence s = readNumber(firstChar);
		return Long.parseLong(s, 0, s.length(), 10);
	}

	private double readDecimal(int firstChar) throws IOException {
		return Double.parseDouble(readNumber(firstChar).toString());
	}

	private Number readBigNumber(int firstChar) throws IOException {
		String string = readNumber(firstChar).toString();
		return new BigDecimal(string);
	}

	/**
	 * Parsing numbers is a pain in the ass for many reasons.
	 * Just get the digits into a {@link CharSequence} and get on with life.
	 */
	private StringBuilder readNumber(int firstChar) throws IOException {
		assert Token.startingWith(firstChar) == NUMBER;
		StringBuilder sb = new StringBuilder();
		sb.appendCodePoint(firstChar);
		for (int c = read(); ; c = read()) {
			switch (c) {
				case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
					 '-', '.', 'e', 'E' -> sb.appendCodePoint(c);
				default -> {
					// And suddenly, without warning, we have walked off
					// the end of the number.
					input.skip(-1);
					return sb;
				}
			}
		}
	}

}
