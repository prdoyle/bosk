package works.bosk.boson.codec.io;

import java.io.ByteArrayInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import works.bosk.boson.codec.JsonReader;
import works.bosk.boson.exceptions.JsonSyntaxException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static works.bosk.boson.codec.Token.NUMBER;
import static works.bosk.boson.codec.Token.START_ARRAY;
import static works.bosk.boson.codec.Token.START_OBJECT;
import static works.bosk.boson.codec.Token.STRING;

class JsonReaderSyntaxExceptionTest {

	@Test
	void trailingCommaInArray() {
		try (JsonReader reader = readerFor("[1,2,]")) {
			assertEquals(START_ARRAY, reader.peekToken());
			reader.consumeFixedToken(START_ARRAY);
			assertEquals(NUMBER, reader.peekToken());
			reader.consumeNumber();
			assertEquals(NUMBER, reader.peekToken());
			reader.consumeNumber();
			assertThrows(JsonSyntaxException.class, reader::peekToken);
		}
	}

	@Test
	void trailingCommaInObject() {
		try (JsonReader reader = readerFor("{\"a\":1,}")) {
			assertEquals(START_OBJECT, reader.peekToken());
			reader.consumeFixedToken(START_OBJECT);
			assertEquals(STRING, reader.peekToken());
			reader.consumeString();
			assertEquals(NUMBER, reader.peekToken());
			reader.consumeNumber();
			assertThrows(JsonSyntaxException.class, () -> reader.peekToken());
		}
	}

	@Test
	void missingCommaBetweenArrayItems() {
		try (JsonReader reader = readerFor("[1 2]")) {
			assertEquals(START_ARRAY, reader.peekToken());
			reader.consumeFixedToken(START_ARRAY);
			assertEquals(NUMBER, reader.peekToken());
			reader.consumeNumber();
			// Next token is another NUMBER but a comma was required between items
			assertThrows(JsonSyntaxException.class, reader::peekToken);
		}
	}

	@Test
	void unclosedArray() {
		try (JsonReader reader = readerFor("[1,2")) {
			assertEquals(START_ARRAY, reader.peekToken());
			reader.consumeFixedToken(START_ARRAY);
			assertEquals(NUMBER, reader.peekToken());
			reader.consumeNumber();
			assertEquals(NUMBER, reader.peekToken());
			reader.consumeNumber();
			// EOF reached before closing bracket
			assertThrows(JsonSyntaxException.class, reader::peekToken);
		}
	}

	@Test
	void unclosedObject() {
		try (JsonReader reader = readerFor("{\"a\":1")) {
			assertEquals(START_OBJECT, reader.peekToken());
			reader.consumeFixedToken(START_OBJECT);
			assertEquals(STRING, reader.peekToken());
			reader.consumeString();
			assertEquals(NUMBER, reader.peekToken());
			reader.consumeNumber();
			// EOF reached before closing brace
			assertThrows(JsonSyntaxException.class, reader::peekToken);
		}
	}

	@Test
	void multipleTopLevelValues() {
		try (JsonReader reader = readerFor("1 2")) {
			assertEquals(NUMBER, reader.peekToken());
			reader.consumeNumber();
			// Another value after a complete top-level value should be a syntax error
			assertThrows(JsonSyntaxException.class, reader::peekToken);
		}
	}

	@Test
	void missingMemberNameInObject() {
		try (JsonReader reader = readerFor("{:1}")) {
			assertEquals(START_OBJECT, reader.peekToken());
			reader.consumeFixedToken(START_OBJECT);
			// Object member name is missing (we encountered ':' instead of a STRING)
			assertThrows(JsonSyntaxException.class, reader::peekToken);
		}
	}

	@Test
	void missingValueAfterColon() {
		try (JsonReader reader = readerFor("{\"a\":}")) {
			assertEquals(START_OBJECT, reader.peekToken());
			reader.consumeFixedToken(START_OBJECT);
			assertEquals(STRING, reader.peekToken());
			reader.consumeString();
			// After the colon there is no value
			assertThrows(JsonSyntaxException.class, reader::peekToken);
		}
	}

	@Test
	void missingColonBetweenNameAndValue() {
		try (JsonReader reader = readerFor("{\"a\" 1}")) {
			assertEquals(START_OBJECT, reader.peekToken());
			reader.consumeFixedToken(START_OBJECT);
			assertEquals(STRING, reader.peekToken());
			reader.consumeString();
			// A ':' is required between name and value
			assertThrows(JsonSyntaxException.class, reader::peekToken);
		}
	}

	@ParameterizedTest
	@ValueSource(strings = {
		"[,]",      // missing first value
		"{,}",      // malformed object contents
	})
	void missingValueAtStart(String json) {
		try (JsonReader reader = readerFor(json)) {
			// Start token is either START_ARRAY or START_OBJECT; parsing their contents should fail
			reader.peekToken();
			assertThrows(JsonSyntaxException.class, reader::peekToken);
		}
	}

	/**
	 * Helper to create a JsonReader for a string
	 */
	private JsonReader readerFor(String json) {
		ByteArrayInputStream in = new ByteArrayInputStream(json.getBytes(UTF_8));
		return JsonReader.create(in).withValidation();
	}
}
