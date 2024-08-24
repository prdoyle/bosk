package works.bosk.driver;

import works.bosk.BoskDriver;

public interface Label {
	Class<? extends BoskDriver> driverClass();
}
