package works.bosk.boson.exceptions;

/**
 * The base class for exceptions that occur during JSON parsing.
 */
public sealed abstract class JsonParseException extends RuntimeException permits JsonFormatException, JsonProcessingException {
	protected JsonParseException(String message) {
		super(message);
	}

	protected JsonParseException(Throwable cause) {
		super(cause);
	}

	protected JsonParseException(String message, Throwable cause) {
		super(message, cause);
	}

	@SuppressWarnings("unchecked")
	public static <T extends JsonParseException> T wrap(T exception, String context) {
		String newMessage = context + ": " + exception.getMessage();
		return switch (exception) {
			case JsonProcessingException e -> (T)new JsonProcessingException(newMessage, e);
			case JsonContentException e -> (T)new JsonContentException(newMessage, e);
			case JsonSyntaxException e -> (T)new JsonSyntaxException(newMessage, e);
			case JsonLexicalException e -> (T)new JsonLexicalException(newMessage, e);
		};
	}
}
