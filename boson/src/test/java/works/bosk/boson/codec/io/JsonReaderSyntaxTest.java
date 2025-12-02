package works.bosk.boson.codec.io;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import works.bosk.boson.codec.JsonReader;
import works.bosk.boson.codec.Token;
import works.bosk.boson.exceptions.JsonSyntaxException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static works.bosk.boson.codec.Token.END_OBJECT;
import static works.bosk.boson.codec.Token.NUMBER;
import static works.bosk.boson.codec.Token.START_ARRAY;
import static works.bosk.boson.codec.Token.START_OBJECT;
import static works.bosk.boson.codec.Token.STRING;
import static works.bosk.boson.codec.Token.TRUE;

@ParameterizedClass
@MethodSource("readerSuppliers")
public class JsonReaderSyntaxTest extends AbstractJsonReaderTest {
	@Override
	protected JsonReader readerFor(String json) {
		// We need validation on all readers here
		return super.readerFor(json).withSyntaxValidation();
	}

	@ParameterizedTest
	@ValueSource(strings = {
		"",
		" ",
		"\n\t\r"
	})
	void emptyInput(String json) {
		try (JsonReader reader = readerFor(json)) {
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void missingColonAfterMemberName() {
		try (JsonReader reader = readerFor("{\"a\" 1}")) {
			expect(reader, START_OBJECT, STRING);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void missingCommaBetweenObjectMembers() {
		try (JsonReader reader = readerFor("{\"a\":1 \"b\":2}")) {
			expect(reader, START_OBJECT, STRING, NUMBER);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void missingCommaBetweenArrayElements() {
		try (JsonReader reader = readerFor("[1 2]")) {
			expect(reader, START_ARRAY, NUMBER);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void nonStringMemberName() {
		try (JsonReader reader = readerFor("{1:2}")) {
			expect(reader, START_OBJECT);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken,
				"peekValueToken must refuse to return a token that would be a syntax error");
		}
	}

	@Test
	void missingMemberValueAfterColon() {
		try (JsonReader reader = readerFor("{\"a\":}")) {
			expect(reader, START_OBJECT, STRING);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void unexpectedCommaAfterMemberName() {
		try (JsonReader reader = readerFor("{\"a\",1}")) {
			expect(reader, START_OBJECT, STRING);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void colonWithoutValue() {
		try (JsonReader reader = readerFor("{\"a\":,}")) {
			expect(reader, START_OBJECT, STRING);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void unexpectedCommaAtArrayStart() {
		try (JsonReader reader = readerFor("[,1]")) {
			expect(reader, START_ARRAY);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@ParameterizedTest
	@ValueSource(strings = {
		":",
		",",
		"]",
		"}",
	})
	void unexpectedPunctuationAtTopLevel(String json) {
		try (JsonReader reader = readerFor(json)) {
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void missingClosingBrace() {
		try (JsonReader reader = readerFor("{\"a\":1")) {
			expect(reader, START_OBJECT, STRING, NUMBER);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void missingClosingBracket() {
		try (JsonReader reader = readerFor("[1,2")) {
			expect(reader, START_ARRAY, NUMBER, NUMBER);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void trailingCommaInArray() {
		try (JsonReader reader = readerFor("[1,]")) {
			expect(reader, START_ARRAY, NUMBER);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void missingCommaBetweenArrayElementsInMemberValue() {
		try (JsonReader reader = readerFor("{\"a\":[1 2]}")) {
			expect(reader, START_OBJECT, STRING, START_ARRAY, NUMBER);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void commaImmediatelyAfterStartObject() {
		try (JsonReader reader = readerFor("{,}")) {
			expect(reader, START_OBJECT);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void trailingCommaInObject() {
		try (JsonReader reader = readerFor("{\"a\":1,}")) {
			expect(reader, START_OBJECT, STRING, NUMBER);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void doubleCommaInArray() {
		try (JsonReader reader = readerFor("[1,,2]")) {
			expect(reader, START_ARRAY, NUMBER);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void colonWhereCommaExpectedInArray() {
		try (JsonReader reader = readerFor("[1:2]")) {
			expect(reader, START_ARRAY, NUMBER);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void colonWithoutMemberName() {
		try (JsonReader reader = readerFor("{:1}")) {
			expect(reader, START_OBJECT);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void arrayAsMemberNameIsRejected() {
		try (JsonReader reader = readerFor("{[1]:2}")) {
			expect(reader, START_OBJECT);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void nestedObjectMissingCommaBetweenMembers() {
		try (JsonReader reader = readerFor("[{\"a\":1 \"b\":2}]")) {
			expect(reader, START_ARRAY, START_OBJECT, STRING, NUMBER);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void nestedArrayMissingCommaBetweenElements() {
		try (JsonReader reader = readerFor("[1,[2 3],4]")) {
			expect(reader, START_ARRAY, NUMBER, START_ARRAY, NUMBER);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void trailingCommaInNestedArrayValue() {
		try (JsonReader reader = readerFor("{\"a\":[1,],\"b\":2}")) {
			expect(reader, START_OBJECT, STRING, START_ARRAY, NUMBER);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void missingValueAfterColonNested() {
		try (JsonReader reader = readerFor("{\"a\":{\"b\":}}")) {
			expect(reader, START_OBJECT, STRING, START_OBJECT, STRING);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void extraTopLevelValues() {
		try (JsonReader reader = readerFor("true false null")) {
			expect(reader, TRUE);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void unexpectedClosingInNestedContext() {
		try (JsonReader reader = readerFor("[{\"a\":1}}]")) {
			expect(reader, START_ARRAY, START_OBJECT, STRING, NUMBER, END_OBJECT);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void missingCommaBetweenMemberAndArrayValue() {
		try (JsonReader reader = readerFor("{\"a\":1[\"b\":2]}")) {
			expect(reader, START_OBJECT, STRING, NUMBER);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	@Test
	void missingCommaBetweenMembersNestedDeep() {
		try (JsonReader reader = readerFor("{\"a\":{\"b\":[1 2]}}")) {
			expect(reader, START_OBJECT, STRING, START_OBJECT, STRING, START_ARRAY, NUMBER);
			assertThrows(JsonSyntaxException.class, reader::peekValueToken);
		}
	}

	/**
	 * Lets you skip over parts of the input that are expected to be valid.
	 */
	private void expect(JsonReader reader, Token... tokens) {
		for (Token token : tokens) {
			assertEquals(token, reader.peekValueToken());
			switch (token) {
				case STRING -> reader.consumeString();
				case NUMBER -> reader.consumeNumber();
				default -> reader.consumeFixedToken(token);
			}
		}
	}

}
