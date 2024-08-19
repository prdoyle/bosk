package works.bosk;

import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Test;
import works.bosk.annotations.ReferencePath;
import works.bosk.annotations.VariantCaseMap;
import works.bosk.exceptions.InvalidTypeException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static works.bosk.BoskTestUtils.boskName;

class VariantTest extends AbstractBoskTest {

	public record BoskState(
		TestVariant v
	) implements StateTreeNode { }

	public interface TestVariant extends VariantNode {
		default String tag() {
			if (this instanceof StringCase) {
				return "string";
			} else {
				return "id";
			}
		}

		@VariantCaseMap
		MapValue<Class<? extends TestVariant>> TYPE_MAP = MapValue.copyOf(Map.of(
			"string", StringCase.class,
			"id", IDCase.class
		));
	}

	public record StringCase(String value) implements TestVariant {}
	public record IDCase(Identifier value) implements TestVariant {}

	public interface Refs {
		@ReferencePath("/v") Reference<TestVariant> v();
		@ReferencePath("/v/id") Reference<IDCase> idCase();
		@ReferencePath("/v/id/value") Reference<Identifier> idValue();
		@ReferencePath("/v/string/value") Reference<String> stringValue();
	}

	@Test
	void test() throws InvalidTypeException, IOException, InterruptedException {
		String stringValue = "test";
		var bosk = new Bosk<>(boskName(), BoskState.class, _ -> new BoskState(new StringCase(stringValue)), Bosk::simpleDriver);
		var refs = bosk.rootReference().buildReferences(Refs.class);
		try (var _ = bosk.readContext()) {
			assertEquals(stringValue, refs.stringValue().value());
		}
		IDCase idCase = new IDCase(Identifier.from("test2"));
		bosk.driver().submitReplacement(refs.idCase(), idCase);
		bosk.driver().flush();
		try (var _ = bosk.readContext()) {
			assertEquals(idCase, refs.v().value());
			assertEquals(idCase, refs.idCase().value());
			assertEquals(idCase.value(), refs.idValue().value());
			assertNull(refs.stringValue().valueIfExists());
		}
	}

	public interface WrongTagVariant extends VariantNode {
		@VariantCaseMap
		MapValue<Class<? extends WrongTagVariant>> CASE_MAP = MapValue.copyOf(Map.of(
			"expectedTag", NonexistentTagCase.class
		));
	}

	public record NonexistentTagCase() implements WrongTagVariant {
		public NonexistentTagCase {
			// The assertion recommended in the user's guide
			assert CASE_MAP.get(tag()).isInstance(this);
		}

		@Override
		public String tag() {
			return "actualTag";
		}
	}

	public record WrongTypeCase() implements WrongTagVariant {
		public WrongTypeCase {
			// The assertion recommended in the user's guide
			assert CASE_MAP.get(tag()).isInstance(this);
		}

		@Override
		public String tag() {
			return "expectedTag";
		}
	}

	@Test
	void wrongTag_fails() {
		assertThrows(NullPointerException.class, NonexistentTagCase::new);
		assertThrows(AssertionError.class, WrongTypeCase::new);
	}
}
