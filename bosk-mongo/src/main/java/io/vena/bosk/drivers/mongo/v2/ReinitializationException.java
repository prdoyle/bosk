package io.vena.bosk.drivers.mongo.v2;

public class ReinitializationException extends Exception {
	public ReinitializationException(Throwable cause) {
		super(cause);
	}

	public ReinitializationException(String message) {
		super(message);
	}

	public ReinitializationException(String message, Throwable cause) {
		super(message, cause);
	}
}
