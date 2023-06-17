package io.vena.bosk.drivers.mongo.v3;

public class DisconnectedException extends RuntimeException {
	public DisconnectedException(String message) {
		super(message);
	}

	public DisconnectedException(String message, Throwable cause) {
		super(message, cause);
	}

	public DisconnectedException(Throwable cause) {
		super(cause);
	}
}
