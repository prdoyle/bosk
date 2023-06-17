package io.vena.bosk.drivers.mongo.v3;

public interface ResettableDriver<R> {
	R initializeReplication();
}
