package org.vena.bosk.drivers.mongo;

import org.vena.bosk.Bosk;
import org.vena.bosk.BoskDriver;
import org.vena.bosk.DriverFactory;
import org.vena.bosk.Entity;

public interface MongoDriverFactory<RR extends Entity> extends DriverFactory<RR> {
	@Override MongoDriver<RR> build(Bosk<RR> bosk, BoskDriver<RR> downstream);
}
