package works.bosk.boson.exceptions;

/**
 * The parser has encountered input text that does meet expectations.
 */
public abstract sealed class JsonFormatException extends JsonParseException permits
	JsonValidityException,
	JsonContentException
{
	JsonFormatException(String message) {
		super(message);
	}

	JsonFormatException(Throwable cause) {
		super(cause);
	}

	JsonFormatException(String message, Throwable cause) {
		super(message, cause);
	}
}
