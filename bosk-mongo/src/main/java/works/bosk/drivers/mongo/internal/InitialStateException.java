package works.bosk.drivers.mongo.internal;

/**
 * Thrown to indicate that initial state could not be obtained from one of the
 * initialization sources (MongoDB or the downstream driver).
 */
abstract class InitialStateException extends Exception {
	protected InitialStateException(String message, Throwable cause) {
		super(message, cause);
	}
}
