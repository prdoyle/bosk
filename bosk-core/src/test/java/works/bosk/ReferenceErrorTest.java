package works.bosk;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.FieldNameConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import works.bosk.exceptions.InvalidTypeException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static works.bosk.BoskTestUtils.boskName;

public class ReferenceErrorTest {
	Bosk<?> bosk;

	@BeforeEach
	void setupBosk() {
		bosk = new Bosk<>(
			boskName(),
			BadGetters.class,
			_ -> new BadGetters(Identifier.from("test"), new NestedObject(Optional.of("stringValue"))),
			Bosk.simpleStack());
	}

	@Test
	void referenceGet_brokenGetter_propagatesException() throws InvalidTypeException {
		Reference<Identifier> idRef = bosk.rootReference().then(Identifier.class, Path.just("id"));
		try (var _ = bosk.readContext()) {
			assertThrows(UnsupportedOperationException.class, idRef::value,
				"Reference.value() should propagate the exception as-is");
		}
	}

	@Test
	void referenceUpdate_brokenGetter_propagatesException() throws InvalidTypeException {
		Reference<String> stringRef = bosk.rootReference().then(String.class, Path.of(BadGetters.Fields.nestedObject, NestedObject.Fields.string));
		assertThrows(UnsupportedOperationException.class, ()->
			bosk.driver().submitReplacement(stringRef, "newValue"));
		assertThrows(UnsupportedOperationException.class, ()->
			bosk.driver().submitDeletion(stringRef));
	}

	@RequiredArgsConstructor
	@FieldNameConstants
	public static class BadGetters implements Entity {
		final Identifier id;
		final NestedObject nestedObject;

		public Identifier id() {
			throw new UnsupportedOperationException("Whoops");
		}

		public NestedObject nestedObject() {
			throw new UnsupportedOperationException("Whoops");
		}
	}

	@Value
	@FieldNameConstants
	public static class NestedObject implements StateTreeNode {
		Optional<String> string;
	}
}
