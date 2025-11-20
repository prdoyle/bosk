package works.bosk.boson.codec.io;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Parameter;
import java.util.List;
import java.util.stream.Stream;
import works.bosk.boson.codec.JsonReader;
import works.bosk.boson.exceptions.JsonLexicalException;
import works.bosk.junit.InjectFrom;
import works.bosk.junit.InjectedTest;
import works.bosk.junit.ParameterInjector;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertThrows;

@InjectFrom({
	JsonReaderValidateCharactersTest.ReaderFactoryInjector.class
})
public class JsonReaderValidateCharactersTest {
	private final JsonReader reader;

	JsonReaderValidateCharactersTest(ReaderFactoryParameter p) {
		// We do the happy parts that are expected to pass in here.
		// This doubles as setup for later tests that
		// expect failures, because those ruin the reader.

		reader = p.factory().create("1234567890123456789012345", 6);

		// Stop before chunk boundary
		reader.validateCharacters("1234");
		// Zero-sized string always matches
		reader.validateCharacters("");
		// Cross chunk boundary
		reader.validateCharacters("5678");
		// Stop on chunk boundary
		reader.validateCharacters("9012");
		// Make sure we're still good
		reader.validateCharacters("3");

		// The reader is left with 45678, 901234, and 5 in three chunks
	}

	@InjectedTest
	void wrongImmediately() {
		assertThrows(JsonLexicalException.class,
			() -> reader.validateCharacters("x"));
	}

	@InjectedTest
	void wrongAfterRight() {
		assertThrows(JsonLexicalException.class,
			() -> reader.validateCharacters("456x"));
	}

	@InjectedTest
	void wrongAfterBoundary() {
		assertThrows(JsonLexicalException.class,
			() -> reader.validateCharacters("45678x"));
	}

	@InjectedTest
	void wrongOnLastCharacter() {
		assertThrows(JsonLexicalException.class,
			() -> reader.validateCharacters("45678901234x"));
	}

	@InjectedTest
	void rightToTheEnd() {
		reader.validateCharacters("456789012345");
		// Empty string matches even at the end
		reader.validateCharacters("");
	}

	@InjectedTest
	void rightButTooLong() {
		assertThrows(JsonLexicalException.class,
			() -> reader.validateCharacters("456789012345x"));
	}

	interface ReaderFactory {
		JsonReader create(String json, int chunkSize);
	}

	/**
	 * Gives meaningful names to reader factories so it's easier
	 * to understand the test reports.
	 */
	record ReaderFactoryParameter(String name, ReaderFactory factory) {
		@Override
		public String toString() {
			return name;
		}
	}

	public record ReaderFactoryInjector() implements ParameterInjector {

		@Override
		public boolean supportsParameter(Parameter parameter) {
			return parameter.getType().equals(ReaderFactoryParameter.class);
		}

		@Override
		public List<Object> values() {
			return Stream.of(
				new ReaderFactoryParameter("overlapped", (json, chunkSize) -> {
					var filler = new OverlappedPrefetchingChunkFiller(
						new ByteArrayInputStream(json.getBytes(UTF_8)),
						chunkSize, 2
					);
					return new ByteChunkJsonReader(filler);
				}),
				new ReaderFactoryParameter("synchronous", (json, chunkSize) -> {
					var filler = new SynchronousChunkFiller(
						new ByteArrayInputStream(json.getBytes(UTF_8)),
						chunkSize
					);
					return new ByteChunkJsonReader(filler);
				}),
				new ReaderFactoryParameter("char array", (json, _) -> {
					char[] chars = json.toCharArray();
					return new CharArrayJsonReader(chars);
				})
			).map(f -> (Object) f).toList();
		}
	}

}
