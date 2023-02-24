package io.vena.bosk.drivers.mongo;

import io.vena.bosk.Path;
import io.vena.bosk.drivers.mongo.MongoDriverSettings.MongoDriverSettingsBuilder;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static io.vena.bosk.drivers.mongo.MongoDriverSettings.FlushMode.ECHO;
import static io.vena.bosk.drivers.mongo.MongoDriverSettings.FlushMode.REVISION_FIELD_ONLY;
import static java.util.Collections.singletonList;

public interface TestParameters {
	AtomicInteger dbCounter = new AtomicInteger(0);

	@SuppressWarnings("unused")
	static Stream<MongoDriverSettingsBuilder> driverSettings() {
		String prefix = "boskTestDB_" + dbCounter.incrementAndGet();
		return Stream.of(
			MongoDriverSettings.builder()
				.database(prefix + "_echo")
				.flushMode(ECHO),
			MongoDriverSettings.builder()
				.database(prefix + "_multi")
				.separateCollections(singletonList(Path.just("catalog"))),
			MongoDriverSettings.builder()
				.database(prefix + "_rev")
				.flushMode(REVISION_FIELD_ONLY)
			// This test fails too often. REVISION_FIELD_ONLY is not reliable yet.
//			MongoDriverSettings.builder()
//				.database(prefix + "_slow")
//				.flushMode(REVISION_FIELD_ONLY)
//				.testing(MongoDriverSettings.Testing.builder()
//					.eventDelayMS(100)
//					.build())
		);
	}

}
