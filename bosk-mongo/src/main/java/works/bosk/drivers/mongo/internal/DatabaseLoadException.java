package works.bosk.drivers.mongo.internal;

/**
 * Thrown to indicate that the initial state could not be loaded from MongoDB.
 * <p>
 * Note that this is not necessarily fatal to the Bosk constructor:
 * in {@link works.bosk.drivers.mongo.MongoDriverSettings.InitialDatabaseUnavailableMode#DISCONNECT DISCONNECT} mode,
 * the constructor can fall back to the downstream driver's initial state.
 * In that case, this exception is used on the {@link ChangeReceiver} thread to signal
 * that it should attempt a reconnection.
 */
class DatabaseLoadException extends InitialStateException {
	DatabaseLoadException(String message, Throwable cause) {
		super(message, cause);
	}

	DatabaseLoadException(String message) {
		this(message, null);
	}
}
