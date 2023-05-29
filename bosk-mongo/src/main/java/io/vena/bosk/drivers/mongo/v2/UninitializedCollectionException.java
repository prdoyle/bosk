package io.vena.bosk.drivers.mongo.v2;

/**
 * Thrown when the database doesn't contain the expected contents.
 * In some cases, the caller should create and populate the database;
 * in others, this is an error.
 */
public class UninitializedCollectionException extends Exception {
	public UninitializedCollectionException() {
	}

	public UninitializedCollectionException(String message) {
		super(message);
	}

	public UninitializedCollectionException(String message, Throwable cause) {
		super(message, cause);
	}

	public UninitializedCollectionException(Throwable cause) {
		super(cause);
	}
}
