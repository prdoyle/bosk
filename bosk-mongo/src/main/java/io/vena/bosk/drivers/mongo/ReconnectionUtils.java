package io.vena.bosk.drivers.mongo;

import com.mongodb.MongoClientSettings;
import io.vena.bosk.Bosk;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.Entity;

class ReconnectionUtils {
	public static <RR extends Entity> SingleDocumentMongoDriver<RR> reconnectDriver(MongoClientSettings clientSettings, MongoDriverSettings driverSettings, BsonPlugin bsonPlugin, Bosk<RR> b, BoskDriver<RR> d) {
		// Pretend to check the DB manifest and select the right implementation
		// TODO: Actually do this ^
		return new SingleDocumentMongoDriver<>(
			b,
			clientSettings,
			driverSettings,
			bsonPlugin,
			d);
	}
}
