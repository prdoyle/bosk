package org.vena.bosk.drivers.mongo;

interface Upgrader<OLD,NEW> {
	NEW upgradeFrom(OLD obj);
	OLD downgradeFrom(NEW obj);
}
