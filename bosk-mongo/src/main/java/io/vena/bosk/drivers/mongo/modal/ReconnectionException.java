package io.vena.bosk.drivers.mongo.modal;

public class ReconnectionException extends RuntimeException {
	public ReconnectionException(Throwable cause) {
		super(cause);
	}

	public ReconnectionException(String message) {
		super(message);
	}

	public ReconnectionException(String message, Throwable cause) {
		super(message, cause);
	}
}
