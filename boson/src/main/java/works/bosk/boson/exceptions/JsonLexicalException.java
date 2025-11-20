package works.bosk.boson.exceptions;

/**
 * The parser has encountered input text that does not correspond
 * to any stream of {@link works.bosk.boson.codec.Token Token}s.
 * This would include such errors as invalid string or number
 * syntax, non-ascii characters outside strings, or uppercase
 * literals like "True" instead of "true".
 */
public final class JsonLexicalException extends JsonValidityException {
	public JsonLexicalException(String message) {
		super(message);
	}

	public JsonLexicalException(Throwable cause) {
		super(cause);
	}

	public JsonLexicalException(String message, Throwable cause) {
		super(message, cause);
	}
}
