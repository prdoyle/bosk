package works.bosk.json.codec.io;

import java.io.ByteArrayInputStream;
import org.junit.jupiter.api.Test;

import static java.nio.channels.Channels.newChannel;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static works.bosk.json.mapping.Token.END_ARRAY;
import static works.bosk.json.mapping.Token.END_OBJECT;
import static works.bosk.json.mapping.Token.END_TEXT;
import static works.bosk.json.mapping.Token.FALSE;
import static works.bosk.json.mapping.Token.NULL;
import static works.bosk.json.mapping.Token.NUMBER;
import static works.bosk.json.mapping.Token.START_ARRAY;
import static works.bosk.json.mapping.Token.START_OBJECT;
import static works.bosk.json.mapping.Token.STRING;
import static works.bosk.json.mapping.Token.TRUE;

class JsonReaderTest {

	@Test
	void simpleString() {
		try (JsonReader reader = readerFor("\"hello\"")) {
			assertEquals(STRING, reader.nextToken());
			assertEquals("hello", reader.readString());
			assertEquals(END_TEXT, reader.nextToken());
		}
	}

	@Test
	void stringWithEscapes() {
		try (JsonReader reader = readerFor("\"he\\\"llo\\nworld\\\\\"")) {
			assertEquals(STRING, reader.nextToken());
			assertEquals("he\"llo\nworld\\", reader.readString());
		}
	}

	@Test
	void unicodeEscape() {
		try (JsonReader reader = readerFor("\"\\u0041\\u0042\\u0043\"")) {
			assertEquals(STRING, reader.nextToken());
			assertEquals("ABC", reader.readString());
		}
	}

	@Test
	void emptyString() {
		try (JsonReader reader = readerFor("\"\"")) {
			assertEquals(STRING, reader.nextToken());
			assertEquals("", reader.readString());
		}
	}

	@Test
	void numberToken() {
		try (JsonReader reader = readerFor("12345")) {
			assertEquals(NUMBER, reader.nextToken());
			assertEquals("12345", reader.numberChars().toString());
		}
	}

	@Test
	void negativeAndFractionalNumber() {
		try (JsonReader reader = readerFor("-12.34e+5")) {
			assertEquals(NUMBER, reader.nextToken());
			assertEquals("-12.34e+5", reader.numberChars().toString());
		}
	}

	@Test
	void structuralTokens() {
		try (JsonReader reader = readerFor("{\"a\": [1, 2]}")) {
			assertEquals(START_OBJECT, reader.nextToken());
			assertEquals(STRING, reader.nextToken());
			assertEquals("a", reader.readString());
			assertEquals(START_ARRAY, reader.nextToken());
			assertEquals(NUMBER, reader.nextToken());
			assertEquals("1", reader.numberChars().toString());
			assertEquals(NUMBER, reader.nextToken());
			assertEquals("2", reader.numberChars().toString());
			assertEquals(END_ARRAY, reader.nextToken());
			assertEquals(END_OBJECT, reader.nextToken());
			assertEquals(END_TEXT, reader.nextToken());
		}
	}

	@Test
	void trueFalseNull() {
		try (JsonReader reader = readerFor("[true,false,null]")) {
			assertEquals(START_ARRAY, reader.nextToken());
			assertEquals(TRUE, reader.nextToken());
			assertEquals(FALSE, reader.nextToken());
			assertEquals(NULL, reader.nextToken());
			assertEquals(END_ARRAY, reader.nextToken());
			assertEquals(END_TEXT, reader.nextToken());
		}
	}

	@Test
	void stringWithAllEscapes() {
		try (JsonReader reader = readerFor("\"\\\"\\\\\\/\\b\\f\\n\\r\\t\"")) {
			assertEquals(STRING, reader.nextToken());
			assertEquals("\"\\/\b\f\n\r\t", reader.readString());
		}
	}

	@Test
	void unterminatedStringThrows() {
		try (JsonReader reader = readerFor("\"abc")) {
			assertEquals(STRING, reader.nextToken());
			assertThrows(IllegalStateException.class, reader::readString);
		}
	}

	@Test
	void invalidEscapeThrows() {
		try (JsonReader reader = readerFor("\"abc\\x\"")) {
			assertEquals(STRING, reader.nextToken());
			assertThrows(IllegalStateException.class, reader::readString);
		}
	}

	/**
	 * Helper to create a JsonReader for a string
	 */
	private JsonReader readerFor(String json) {
		ByteArrayInputStream in = new ByteArrayInputStream(json.getBytes(UTF_8));
		return JsonReader.create(newChannel(in));
	}
}
