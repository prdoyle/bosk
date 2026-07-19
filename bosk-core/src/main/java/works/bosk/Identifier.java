package works.bosk;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.jspecify.annotations.NonNull;

/**
 * The means by which {@link Entity entities} are identified within
 * a particular domain, such as a {@link Catalog} or {@link SideTable}.
 */
public final class Identifier {
	@NonNull
	final String value;

	private Identifier(@NonNull String value) {
		this.value = value;
	}

	static final int MAX_LENGTH = 100;

	// TODO: Intern these.  No need to have several Identifier objects for the same value
	public static Identifier from(String value) {
		if (value.isEmpty()) {
			throw new IllegalArgumentException("Identifier can't be empty");
		}
		if (value.length() > MAX_LENGTH) {
			throw new IllegalArgumentException(
				"Identifier too long (max " + MAX_LENGTH + " characters)");
		}

		char first = value.charAt(0);
		checkBoundaryCharacter(first, "start");

		if (value.length() > 1) {
			char last = value.charAt(value.length() - 1);
			checkBoundaryCharacter(last, "end");

			checkNotBoundaryModifier(value.charAt(1), "second");
			checkNotBoundaryModifier(value.charAt(value.length() - 2), "second-last");
		}

		// Hyphens are implicitly rejected in boundary positions:
		// DASH_PUNCTUATION is not in isAllowedBoundaryChar

		return new Identifier(value);
	}

	private static void checkBoundaryCharacter(char c, String position) {
		if (!isAllowedBoundaryChar(c)) {
			throw new IllegalArgumentException(
				"Identifier can't " + position + " with " + describeChar(c));
		}
	}

	private static void checkNotBoundaryModifier(char c, String position) {
		if (isBoundaryModifier(c)) {
			throw new IllegalArgumentException(
				"Identifier's " + position + " character can't modify a boundary character: "
					+ describeChar(c));
		}
	}

	private static boolean isAllowedBoundaryChar(char c) {
		return switch (Character.getType(c)) {
			case Character.UPPERCASE_LETTER,
				Character.LOWERCASE_LETTER,
				Character.TITLECASE_LETTER,
				Character.MODIFIER_LETTER,
				Character.OTHER_LETTER,
				Character.DECIMAL_DIGIT_NUMBER,
				Character.LETTER_NUMBER,
				Character.OTHER_NUMBER,
				Character.CONNECTOR_PUNCTUATION,
				Character.OTHER_PUNCTUATION,
				Character.CURRENCY_SYMBOL -> true;
			default -> false;
		};
	}

	private static boolean isBoundaryModifier(char c) {
		return switch (Character.getType(c)) {
			case Character.NON_SPACING_MARK,
				Character.COMBINING_SPACING_MARK,
				Character.ENCLOSING_MARK,
				Character.FORMAT -> true;
			default -> false;
		};
	}

	private static String describeChar(char c) {
		String name = Character.getName(c);
		return c > 0x20 && c <= 0x7F
			? "'" + c + "'"
			: name != null ? name : String.format("U+%04X", (int) c);
	}

	/**
	 * I'm going to regret adding this.
	 */
	public static Identifier unique(String prefix) {
		return new Identifier(prefix + (uniqueIdCounter.incrementAndGet()));
	}

	private static final AtomicLong uniqueIdCounter = new AtomicLong(1000);

	@Override
	public String toString() {
		return value;
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Identifier that = (Identifier) o;
		return Objects.equals(value, that.value);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(value);
	}
}
