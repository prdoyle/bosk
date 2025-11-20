package works.bosk.boson.exceptions;

/**
 * The parser has encountered input text corresponding to a stream
 * of {@link works.bosk.boson.codec.Token Token}s that do not correspond
 * to a valid JSON document.
 * This would include such errors as misplaced or missing separators,
 * unclosed arrays or objects, or non-string values where member
 * names are expected.
 */
public final class JsonSyntaxException extends JsonValidityException {
	public JsonSyntaxException(String message) {
		super(message);
	}

	public JsonSyntaxException(Throwable cause) {
		super(cause);
	}

	public JsonSyntaxException(String message, Throwable cause) {
		super(message, cause);
	}
}
