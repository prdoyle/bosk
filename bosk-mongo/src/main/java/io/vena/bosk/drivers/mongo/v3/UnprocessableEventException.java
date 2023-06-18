package io.vena.bosk.drivers.mongo.v3;

import io.vena.bosk.drivers.mongo.v3.FormatDriver;

/**
 * Indicate that no {@link FormatDriver} could cope with a particular
 * change stream event. The framework responds with a (potentially expensive)
 * reload operation that avoids attempting to re-process that event;
 * in other words, using resume tokens would never be appropriate for these.
 *
 * @see UnexpectedEventProcessingException
 */
public class UnprocessableEventException extends Exception {
	public UnprocessableEventException(String message) {
		super(message);
	}

	public UnprocessableEventException(String message, Throwable cause) {
		super(message, cause);
	}

	public UnprocessableEventException(Throwable cause) {
		super(cause);
	}
}
