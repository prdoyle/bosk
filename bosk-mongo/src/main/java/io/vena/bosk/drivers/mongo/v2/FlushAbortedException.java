package io.vena.bosk.drivers.mongo.v2;

import io.vena.bosk.exceptions.FlushFailureException;

/**
 * A special kind of {@link FlushFailureException} indicating that
 * the {@link FlushLock} was closed as part of a failure recovery
 * operation. In that particular case, it makes sense to retry
 * the flush again immediately, much like for {@link DisconnectedException}.
 */
public class FlushAbortedException extends FlushFailureException {
	public FlushAbortedException(String message) {
		super(message);
	}

	public FlushAbortedException(String message, Throwable cause) {
		super(message, cause);
	}

	public FlushAbortedException(Throwable cause) {
		super(cause);
	}
}
