package works.bosk.exceptions;

import java.time.Duration;
import works.bosk.BoskDriver;

import static java.util.Objects.requireNonNull;

/**
 * Indicates that a call to {@link BoskDriver#flush()} was unable to complete
 * because the driver cannot currently confirm that the state reflects all prior
 * updates, but a later attempt may succeed.
 * <p>
 * This is not a claim that the driver is broken, that the request is invalid, or
 * that the condition is guaranteed to clear. It is the driver's way of saying
 * "not now; try again later", and {@link #retryAfter()} is its estimate of how long
 * to wait. The hint is not a promise that a retry will succeed.
 * <p>
 * This is a {@link FlushFailureException} because it is still a failure to flush.
 */
public class RetryableFlushException extends FlushFailureException {
	private final Duration retryAfter;

	public RetryableFlushException(String message, Duration retryAfter) {
		super(message);
		this.retryAfter = requireNonNull(retryAfter);
	}

	public RetryableFlushException(String message, Throwable cause, Duration retryAfter) {
		super(message, cause);
		this.retryAfter = requireNonNull(retryAfter);
	}

	public RetryableFlushException(Throwable cause, Duration retryAfter) {
		super(cause);
		this.retryAfter = requireNonNull(retryAfter);
	}

	/**
	 * @return the driver's estimate of how long to wait before retrying
	 */
	public Duration retryAfter() {
		return retryAfter;
	}
}
