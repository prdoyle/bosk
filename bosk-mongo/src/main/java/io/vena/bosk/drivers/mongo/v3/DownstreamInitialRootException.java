package io.vena.bosk.drivers.mongo.v3;

import io.vena.bosk.drivers.mongo.MongoDriver;
import java.util.concurrent.FutureTask;

/**
 * Unlike other exceptions we use internally, this one is a {@link RuntimeException}
 * because it's thrown from a {@link FutureTask}, and those can't throw checked exceptions.
 * Callers of {@link FutureTask#get()} implementing {@link MongoDriver#initialRoot}
 * need to handle this appropriately without any help from the compiler.
 */
public class DownstreamInitialRootException extends IllegalStateException {
	public DownstreamInitialRootException(String message) {
		super(message);
	}

	public DownstreamInitialRootException(String message, Throwable cause) {
		super(message, cause);
	}

	public DownstreamInitialRootException(Throwable cause) {
		super(cause);
	}
}
