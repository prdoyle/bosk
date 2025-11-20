package works.bosk.boson.exceptions;

/**
 * The parser has encountered text that does not conform to the JSON standard.
 */
public abstract sealed class JsonValidityException extends JsonFormatException permits
	JsonLexicalException,
	JsonSyntaxException
{
	public JsonValidityException(String message) {
		super(message);
	}

	public JsonValidityException(Throwable cause) {
		super(cause);
	}

	public JsonValidityException(String message, Throwable cause) {
		super(message, cause);
	}
}
