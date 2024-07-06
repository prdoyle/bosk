package works.bosk.drivers.mongo;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import works.bosk.DriverStack;
import works.bosk.drivers.HanoiTest;
import works.bosk.junit.ParametersByName;

@UsesMongoService
public class MongoDriverHanoiTest extends HanoiTest {
	private static MongoService mongoService;
	private Queue<Runnable> shutdownOperations = new ConcurrentLinkedDeque<>();

	@ParametersByName
	public MongoDriverHanoiTest(TestParameters.ParameterSet parameters) {
		MongoDriverSettings settings = parameters.driverSettingsBuilder().build();
		this.driverFactory = DriverStack.of(
			(b,d) -> { shutdownOperations.add(((MongoDriver<?>)d)::close); return d;},
			MongoDriver.factory(
				mongoService.clientSettings(),
				settings,
				new BsonPlugin()
			)
		);
		mongoService.client()
			.getDatabase(settings.database())
			.drop();
	}

	@BeforeAll
	static void setupMongoConnection() {
		mongoService = new MongoService();
	}

	@SuppressWarnings("unused")
	static Stream<TestParameters.ParameterSet> parameters() {
		return TestParameters.driverSettings(
			Stream.of(
				PandoFormat.oneBigDocument(),
				PandoFormat.withGraftPoints("/puzzles"),
				PandoFormat.withGraftPoints("/puzzles/-puzzle-/towers"),
				PandoFormat.withGraftPoints("/puzzles", "/puzzles/-puzzle-/towers/-tower-/discs"),
				MongoDriverSettings.DatabaseFormat.SEQUOIA
			),
			Stream.of(TestParameters.EventTiming.NORMAL)
		);
	}

}
