package io.vena.bosk.drivers.mongo;

import com.mongodb.MongoClientSettings;
import io.vena.bosk.Bosk;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.DriverFactory;
import io.vena.bosk.DriverStack;
import io.vena.bosk.Entity;
import io.vena.bosk.drivers.mongo.modal.ModalDriverFacade;
import io.vena.bosk.drivers.mongo.modal.ReconnectingModeDriver;

import static io.vena.bosk.drivers.mongo.ReconnectionUtils.reconnectDriver;

public interface MongoDriver<R extends Entity> extends BoskDriver<R> {
	/**
	 * Deserializes and re-serializes the entire bosk contents,
	 * thus updating the database to match the current serialized format.
	 *
	 * <p>
	 * Used to "upgrade" the database contents for schema evolution.
	 *
	 * <p>
	 * This method does not simply write the current in-memory bosk contents
	 * back into the database, because that would lead to race conditions
	 * with other update operations.
	 * Instead, in a causally-consistent transaction, it reads the current
	 * database state, deserializes it, re-serializes it, and writes it back.
	 * This produces predictable results even if done concurrently with
	 * other database updates.
	 */
	void refurbish();

	/**
	 * Frees up resources used by this driver and leaves it unusable.
	 *
	 * <p>
	 * This is done on a best-effort basis. It's more useful for tests than for production code,
	 * where there's usually no reason to close a driver.
	 */
	void close();

	static <RR extends Entity> MongoDriverFactory<RR> factory(
		MongoClientSettings clientSettings,
		MongoDriverSettings driverSettings,
		BsonPlugin bsonPlugin
	) {
		return DriverStack.<RR>of(
			ModalDriverFacade.factory(),
			(b,d) -> new ReconnectingModeDriver<RR>(() -> {
				SingleDocumentMongoDriver<RR> result = (SingleDocumentMongoDriver<RR>) reconnectDriver(clientSettings, driverSettings, bsonPlugin, b, d);
				return result;
			})
		);
	}

	interface MongoDriverFactory<RR extends Entity> extends DriverFactory<RR> {
		@Override MongoDriver<RR> build(Bosk<RR> bosk, BoskDriver<RR> downstream);
	}

}
