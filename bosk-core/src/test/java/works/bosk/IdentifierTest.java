package works.bosk;

import works.bosk.junit.InjectFrom;
import works.bosk.junit.InjectedTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@InjectFrom({IdentifierTest.ValidIdentifier.class, IdentifierTest.InvalidIdentifier.class})
class IdentifierTest {

	@InjectedTest
	void validString_survivesRoundTrip(ValidIdentifier id) {
		assertEquals(id.text, Identifier.from(id.text).toString(), "Should work: " + id.description);
	}

	@InjectedTest
	void invalidString_throws(InvalidIdentifier id) {
		assertThrows(IllegalArgumentException.class, () -> Identifier.from(id.text), "Should be rejected: " + id.description);
	}

	enum ValidIdentifier {
		TEST("test", "simple ASCII"),
		NAME_WITH_SPACES("name with spaces", "spaces in interior"),
		NAME_WITH_SLASHES("name/with/slashes", "OTHER_PUNCTUATION in interior"),
		NAME_WITH_DOTS("name.with.dots", "OTHER_PUNCTUATION in interior"),
		NAME_WITH_NEWLINES("name\nwith\nnewlines", "CONTROL in interior"),
		NAME_WITH_TABS("name\twith\ttabs", "CONTROL character in interior"),
		LEADING_UNDERSCORE("_leadingUnderscore", "CONNECTOR_PUNCTUATION at start"),
		TRAILING_UNDERSCORE("trailingUnderscore_", "CONNECTOR_PUNCTUATION at end"),
		LEADING_DOT(".leadingDot", "OTHER_PUNCTUATION at start"),
		TRAILING_DOT("trailingDot.", "OTHER_PUNCTUATION at end"),
		LEADING_DOLLAR("$leadingDollar", "CURRENCY_SYMBOL at start"),
		TRAILING_DOLLAR("trailingDollar$", "CURRENCY_SYMBOL at end"),
		LEADING_NUMBER("9leadingNumber", "DECIMAL_DIGIT_NUMBER at start"),
		TRAILING_NUMBER("trailingNumber9", "DECIMAL_DIGIT_NUMBER at end"),
		PRECOMPOSED_ACCENT("\u00C1bc", "precomposed accented LETTER at start"),
		UUID("550e8400-e29b-41d4-a716-446655440000", "UUID hex with hyphens"),
		IPV4("192.168.1.1", "IPv4 address"),
		IPV6("2001:db8::1", "IPv6 address"),
		IPV6_LOOPBACK("::1", "IPv6 loopback starts with OTHER_PUNCTUATION"),
		URL("https://example.com", "URL with alphanumeric boundary"),
		URL_WITH_TRAILING_SLASH("https://example.com/", "URL ends with OTHER_PUNCTUATION"),
		SHA256("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "SHA-256 hex digest"),
		GIT_COMMIT("abc123def456abc123def456abc123def456abc1", "Git commit SHA"),
		SEMVER("v1.2.3", "semantic version"),
		HEX_COLOR("#FFAABB", "hex color starts with OTHER_PUNCTUATION"),
		EMAIL_LIKE("user@example.com", "looks like an email address"),
		JWT("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jBBt9F8FTQpL5E1P7oNId6oIlg8nDk0", "JWT token with three dot-separated parts"),
		BASE64("U3VwZXIgbG9uZyBzdHJpbmcgd2l0aCBwYWRkaW5n", "Base64 with LETTER and DIGIT boundaries"),
		MAX_LENGTH("a".repeat(Identifier.MAX_LENGTH), "exactly " + Identifier.MAX_LENGTH + " characters"),
		;

		final String text;
		final String description;

		ValidIdentifier(String text, String description) {
			this.text = text;
			this.description = description;
		}
	}

	enum InvalidIdentifier {
		EMPTY("", "empty string"),
		DASH_AT_START("-startsWithDash", "DASH_PUNCTUATION at start"),
		DASH_AT_END("endsWithDash-", "DASH_PUNCTUATION at end"),
		DASH_AT_BOTH("-startsAndEndsWithDash-", "DASH_PUNCTUATION at boundaries"),
		NON_BMP_AT_END("unicode\uD83C\uDF33", "non-BMP surrogate at end"),
		COMBINING_AT_SECOND("a\u0301bc", "NON_SPACING_MARK at second position"),
		COMBINING_AT_PENULTIMATE("abc\u0301d", "NON_SPACING_MARK at second-last position"),
		COMBINING_AT_START("\u0301abc", "NON_SPACING_MARK at start"),
		COMBINING_AT_END("abc\u0301", "NON_SPACING_MARK at end"),
		CONTROL_AT_START("\u0000abc", "CONTROL at start"),
		NEWLINE_ALONE("\n", "CONTROL as sole character"),
		NEWLINE_AT_START("\nabc", "CONTROL at start"),
		NEWLINE_AT_END("abc\n", "CONTROL at end"),
		SPACE_AT_START(" abc", "SPACE_SEPARATOR at start"),
		SPACE_AT_END("abc ", "SPACE_SEPARATOR at end"),
		LONE_LOW_SURROGATE_AT_START("\uDC00abc", "lone low surrogate at start"),
		LONE_HIGH_SURROGATE_AT_END("abc\uD800", "lone high surrogate at end"),
		TOO_LONG("a".repeat(Identifier.MAX_LENGTH + 1), (Identifier.MAX_LENGTH + 1) + " characters exceeds limit"),
		;

		final String text;
		final String description;

		InvalidIdentifier(String text, String description) {
			this.text = text;
			this.description = description;
		}
	}

}
