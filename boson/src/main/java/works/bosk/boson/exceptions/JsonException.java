package works.bosk.boson.exceptions;

public abstract class JsonException extends RuntimeException {
	public JsonException(String message) {
		super(message);
	}

	public JsonException(Throwable cause) {
		super(cause);
	}

	public JsonException(String message, Throwable cause) {
		super(message, cause);
	}
}
