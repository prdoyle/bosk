package org.vena.bosk.drivers.mongo;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.vena.bosk.AbstractBoskTest;
import org.vena.bosk.Bosk;

public class UpgradeTest extends AbstractBoskTest {
	private static MongoService mongoService;

	@BeforeAll
	static void initialize() {
		mongoService = new MongoService();
	}

	@Test
	@UsesMongoService
	void upgrade_works() {
		Bosk<TestRoot> bosk = setUpBosk((downstream, b) -> {
			MongoDriverSettings driverSettings = MongoDriverSettings.builder()
				.database(UpgradeTest.class.getSimpleName() + "_DB")
				.collection("testCollection")
				.build();
			return new MongoDriver<>(downstream, b, mongoService.clientSettings(), driverSettings, new BsonPlugin());
		});
	}

}
