package io.vena.bosk.drivers.mongo.v2;

import io.vena.bosk.Entity;
import io.vena.bosk.drivers.mongo.MongoDriver;

/**
 * Additional {@link MongoDriver} functionality that the format-specific drivers must implement.
 */
public interface FormatDriver<R extends Entity> extends MongoDriver<R> {
	StateResult<R> loadAllState();
}
