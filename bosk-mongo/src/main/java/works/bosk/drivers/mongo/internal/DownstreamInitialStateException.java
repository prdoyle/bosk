package works.bosk.drivers.mongo.internal;

/**
 * Thrown to indicate that the downstream driver's {@code initialState} failed.
 * <p>
 * This is typically fatal to the Bosk constructor because the downstream {@code initialState}
 * is used as a fallback when the MongoDB initial state is already unavailable.
 */
class DownstreamInitialStateException extends InitialStateException {
	public DownstreamInitialStateException(String message, Throwable cause) {
		super(message, cause);
	}
}
