package works.bosk.boson.exceptions;

/**
 * The parser has encountered JSON text that is syntactically correct but
 * does not conform to the expected specification.
 */
public final class JsonContentException extends JsonFormatException {
	public JsonContentException(String message) {
		super(message);
	}

	public JsonContentException(Throwable cause) {
		super(cause);
	}

	public JsonContentException(String message, Throwable cause) {
		super(message, cause);
	}
}
