package works.bosk.exceptions;

public class TenancyShenanigansException extends RuntimeException {
	public TenancyShenanigansException(String message) {
		super(message);
	}

	public TenancyShenanigansException(String message, Throwable cause) {
		super(message, cause);
	}
}
