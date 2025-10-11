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

		input.expectFixedToken(START_OBJECT);
		loop: while (true) {
			Token token = input.peekToken();
			switch (token) {
				case STRING -> { // member name
					var stringChars = input.processString();
					switch (stringChars.nextChar()) {
						case 'n' -> {
							stringChars.skipToEnd();
							nullField = (String) readAnyValue();
						}
						case 't' -> {
							stringChars.skipToEnd();
							trueField = (Boolean) readAnyValue();
						}
						case 'f' -> {
							stringChars.skipToEnd();
							falseField = (Boolean) readAnyValue();
						}
						case 'i' -> {
							stringChars.skipToEnd();
							integerField = readInteger();
						}
						case 'r' -> {
							stringChars.skipToEnd();
							realField = readDecimal();
						}
						case 'm' -> {
							switch (stringChars.nextChar()) {
								case 'a' -> {
									switch (stringChars.nextChar()) {
										case 'p' -> {
											stringChars.skipToEnd();
											mapField = readTimeUnitToBigDecimalMap();
										}
										case 'y' -> {
											stringChars.skipToEnd();
											maybeAbsentField = readString();
										}
									}
								}
								case 'o' -> {
									stringChars.skipToEnd();
									monthField = Month.fromValue((int) readInteger());
								}
							}
						}
						case 's' -> {
							stringChars.skipChars(5);
							switch (stringChars.nextChar()) {
								case 'F' -> {
									stringChars.skipToEnd();
									stringField = readString();
								}
								case 'A' -> {
									stringChars.skipToEnd();
									stringArrayField = readStringList();
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
		input.consumeFixedToken(END_OBJECT);
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

	private Map<TimeUnit, BigDecimal> readTimeUnitToBigDecimalMap() throws IOException {
		input.expectFixedToken(START_OBJECT);
		Map<TimeUnit, BigDecimal> result = new java.util.LinkedHashMap<>();
		while (input.peekToken() != END_OBJECT) {
			var member = readString();
			var value = readBigNumber();
			result.put(TimeUnit.valueOf(member), (BigDecimal) value);
		}
		input.consumeFixedToken(END_OBJECT);
		return result;
	}

	private List<String> readStringList() {
		input.expectFixedToken(START_ARRAY);
		List<String> result = new java.util.ArrayList<>();
		while (input.peekToken() != Token.END_ARRAY) {
			result.add(input.consumeString());
		}
		input.consumeFixedToken(END_ARRAY);
		return result;
	}

	private List<Object> readAnyList() throws IOException {
		input.expectFixedToken(START_ARRAY);
		List<Object> result = new java.util.ArrayList<>();
		while (input.peekToken() != Token.END_ARRAY) {
			result.add(readAnyValue());
		}
		input.consumeFixedToken(END_ARRAY);
		return result;
	}

	private Map<String, Object> readAnyMap() throws IOException {
		input.expectFixedToken(START_OBJECT);
		Map<String, Object> result = new java.util.LinkedHashMap<>();
		while (input.peekToken() != END_OBJECT) {
			var member = readString();
			var value = readAnyValue();
			result.put(member, value);
		}
		input.consumeFixedToken(END_OBJECT);
		return result;
	}

	private Object readAnyValue() throws IOException {
		switch (input.peekToken()) {
			case NULL -> {
				input.consumeFixedToken(NULL);
				return null;
			}
			case FALSE -> {
				input.consumeFixedToken(FALSE);
				return Boolean.FALSE;
			}
			case TRUE -> {
				input.consumeFixedToken(TRUE);
				return Boolean.TRUE;
			}
			case NUMBER -> {
				return readBigNumber();
			}
			case START_OBJECT -> {
				return readAnyMap();
			}
			case START_ARRAY -> {
				return readAnyList();
			}
			case STRING -> {
				return readString();
			}
			default -> {
				throw new IllegalStateException();
			}
		}
	}

	private String readString() {
		input.peekToken(STRING);
		return input.consumeString();
	}

	private long readInteger() {
		input.peekToken(NUMBER);
		CharSequence s = input.consumeNumber();
		return Long.parseLong(s, 0, s.length(), 10);
	}

	private double readDecimal() {
		input.peekToken(NUMBER);
		return Double.parseDouble(input.consumeNumber().toString());
	}

	private Number readBigNumber() {
		input.peekToken(NUMBER);
		return new BigDecimal(input.consumeNumber().toString());
	}

}
