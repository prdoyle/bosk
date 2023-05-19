package io.vena.bosk.drivers.mongo.v2;

import io.vena.bosk.Entity;
import io.vena.bosk.drivers.mongo.MongoDriver;

public interface FormatDriver<R extends Entity> extends MongoDriver<R> {
	// TODO: Method to fetch current state and revision number
}
