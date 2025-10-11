package works.bosk.json;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import works.bosk.json.TestUtils.Month;
import works.bosk.json.codec.io.JsonReader;
import works.bosk.json.codec.io.JsonStringCharacterReader;
import works.bosk.json.mapping.Token;

import static java.nio.channels.Channels.newChannel;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static works.bosk.json.TestUtils.ABSENT_FIELD_VALUE;
import static works.bosk.json.TestUtils.COMPUTED_FIELD_VALUE;
import static works.bosk.json.TestUtils.ONE_OF_EACH;
import static works.bosk.json.mapping.Token.END_ARRAY;
import static works.bosk.json.mapping.Token.END_OBJECT;
import static works.bosk.json.mapping.Token.FALSE;
import static works.bosk.json.mapping.Token.NULL;
import static works.bosk.json.mapping.Token.NUMBER;
import static works.bosk.json.mapping.Token.START_ARRAY;
import static works.bosk.json.mapping.Token.START_OBJECT;
import static works.bosk.json.mapping.Token.STRING;
import static works.bosk.json.mapping.Token.TRUE;

public class ManualTest {
	/**
	 * Needs pushback only so we can read numbers!
	 */
	private JsonReader input;

	@BeforeEach
	void init() {
		input = JsonReader.create(newChannel(new ByteArrayInputStream(
			ONE_OF_EACH.getBytes(UTF_8))));
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

		input.peekToken(START_OBJECT);
		input.consumeFixedToken(START_OBJECT);
		loop: while (true) {
			Token token = nextToken();
			switch (token) {
				case STRING -> { // member name
					var stringChars = input.processString();
					switch (stringChars.nextChar()) {
						case 'n' -> nullField = (String) readAnyValue(finishMemberName(stringChars));
						case 't' -> trueField = (Boolean) readAnyValue(finishMemberName(stringChars));
						case 'f' -> falseField = (Boolean) readAnyValue(finishMemberName(stringChars));
						case 'i' -> integerField = readInteger(finishMemberName(stringChars));
						case 'r' -> realField = readDecimal(finishMemberName(stringChars));
						case 'm' -> {
							switch (stringChars.nextChar()) {
								case 'a' -> {
									switch (stringChars.nextChar()) {
										case 'p' -> mapField = readTimeUnitToBigDecimalMap(finishMemberName(stringChars));
										case 'y' -> maybeAbsentField = readString(finishMemberName(stringChars));
									}
								}
								case 'o' -> monthField = Month.fromValue((int)readInteger(finishMemberName(stringChars)));
							}
						}
						case 's' -> {
							stringChars.skipChars(5);
							switch (stringChars.nextChar()) {
								case 'F' -> {
									// stringField
									stringField = readString(finishMemberName(stringChars));
								}
								case 'A' -> {
									// stringArrayField
									stringArrayField = readStringList(finishMemberName(stringChars));
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

	private Object finishMemberName(JsonStringCharacterReader charReader) {
		charReader.skipToEnd();
		return null;
	}

	private Map<TimeUnit, BigDecimal> readTimeUnitToBigDecimalMap(Object dummy) throws IOException {
		input.peekToken(START_OBJECT);
		input.consumeFixedToken(START_OBJECT);
		Map<TimeUnit, BigDecimal> result = new java.util.LinkedHashMap<>();
		int c;
		while (input.peekToken() != END_OBJECT) {
			var member = readString(null);
			var value = readBigNumber(null);
			result.put(TimeUnit.valueOf(member), (BigDecimal) value);
		}
		input.consumeFixedToken(END_OBJECT);
		return result;
	}

	private List<String> readStringList(Object dummy) {
		input.peekToken(START_ARRAY);
		input.consumeFixedToken(START_ARRAY);
		List<String> result = new java.util.ArrayList<>();
		while (input.peekToken() != Token.END_ARRAY) {
			result.add(input.consumeString());
		}
		input.consumeFixedToken(END_ARRAY);
		return result;
	}

	private List<Object> readAnyList(Object dummy) throws IOException {
		input.peekToken(START_ARRAY);
		input.consumeFixedToken(START_ARRAY);
		List<Object> result = new java.util.ArrayList<>();
		int c;
		while (input.peekToken() != Token.END_ARRAY) {
			result.add(readAnyValue(null));
		}
		input.consumeFixedToken(END_ARRAY);
		return result;
	}

	private Map<String, Object> readAnyMap(Object dummy) throws IOException {
		input.peekToken(START_OBJECT);
		input.consumeFixedToken(START_OBJECT);
		Map<String, Object> result = new java.util.LinkedHashMap<>();
		while (input.peekToken() != END_OBJECT) {
			var member = readString(null);
			var value = readAnyValue(null);
			result.put(member, value);
		}
		input.consumeFixedToken(END_OBJECT);
		return result;
	}

	/**
	 * When positioned at either the start of a token or an {@link Token#INSIGNIFICANT},
	 * advance to the next character that is not insignificant and return its token.
	 */
	private Token nextToken() {
		return input.peekToken();
	}

	private void skipToken(Token readToken) throws IOException {
		input.consumeFixedToken(readToken);
	}

	private Object readAnyValue(Object dummy) throws IOException {
		switch (input.peekToken()) {
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
				return readBigNumber(dummy);
			}
			case START_OBJECT -> {
				return readAnyMap(dummy);
			}
			case START_ARRAY -> {
				return readAnyList(dummy);
			}
			case STRING -> {
				return readString(dummy);
			}
			default -> {
				throw new IllegalStateException();
			}
		}
	}

	private String readString(Object dummy) {
		input.peekToken(STRING);
		return input.consumeString();
	}

	private long readInteger(Object dummy) throws IOException {
		input.peekToken(NUMBER);
		CharSequence s = input.consumeNumber();
		return Long.parseLong(s, 0, s.length(), 10);
	}

	private double readDecimal(Object dummy) {
		input.peekToken(NUMBER);
		return Double.parseDouble(input.consumeNumber().toString());
	}

	private Number readBigNumber(Object dummy) {
		input.peekToken(NUMBER);
		return new BigDecimal(input.consumeNumber().toString());
	}

}
