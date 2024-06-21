package io.vena.bosk.drivers.mongo;

import io.vena.bosk.DriverStack;
import io.vena.bosk.drivers.HanoiTest;
import io.vena.bosk.drivers.OtelSpanContextDriver;
import io.vena.bosk.drivers.mongo.TestParameters.ParameterSet;
import io.vena.bosk.junit.ParametersByName;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import static io.vena.bosk.Tags.LONG_RUNNING;
import static io.vena.bosk.drivers.mongo.MainDriver.COLLECTION_NAME;
import static io.vena.bosk.drivers.mongo.MongoDriverSettings.DatabaseFormat.SEQUOIA;
import static io.vena.bosk.drivers.mongo.TestParameters.EventTiming.NORMAL;

@Tag(LONG_RUNNING)
public class MongoDriverHanoiTest extends HanoiTest {
	private static MongoService mongoService;

	@ParametersByName
	public MongoDriverHanoiTest(ParameterSet parameters) {
		MongoDriverSettings settings = parameters.driverSettingsBuilder().build();
		this.driverFactory = DriverStack.of(
			OtelSpanContextDriver.factory(),
			MongoDriver.factory(
				mongoService.clientSettings(),
				settings,
				new BsonPlugin()
			),
			OtelSpanContextDriver.factory()
		);
		mongoService.client()
			.getDatabase(settings.database())
			.getCollection(COLLECTION_NAME)
			.drop();
	}

	@BeforeAll
	static void setupMongoConnection() {
		mongoService = new MongoService();
	}

	@SuppressWarnings("unused")
	static Stream<ParameterSet> parameters() {
		return TestParameters.driverSettings(
			Stream.of(
//				PandoFormat.oneBigDocument(),
//				PandoFormat.withGraftPoints("/puzzles"),
//				PandoFormat.withGraftPoints("/puzzles/-puzzle-/towers"),
//				PandoFormat.withGraftPoints("/puzzles", "/puzzles/-puzzle-/towers/-tower-/discs"),
				SEQUOIA
			),
			Stream.of(NORMAL)
		);
	}

}
