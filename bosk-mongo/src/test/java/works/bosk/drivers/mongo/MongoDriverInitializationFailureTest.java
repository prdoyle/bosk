package works.bosk.drivers.mongo;

import org.junit.jupiter.api.Test;
import works.bosk.Bosk;
import works.bosk.drivers.state.TestEntity;

import static ch.qos.logback.classic.Level.ERROR;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests the functionality of {@link MongoDriverSettings.InitialDatabaseUnavailableMode#FAIL FAIL} mode.
 * The other tests in {@link MongoDriverRecoveryTest} exercise {@link MongoDriverSettings.InitialDatabaseUnavailableMode#DISCONNECT DISCONNECT} mode.
 */
public class MongoDriverInitializationFailureTest extends AbstractMongoDriverTest {
	public MongoDriverInitializationFailureTest() {
		super(MongoDriverSettings.builder()
			.database(MongoDriverInitializationFailureTest.class.getSimpleName())
			.experimental(MongoDriverSettings.Experimental.builder()
				.build())
			.initialDatabaseUnavailableMode(MongoDriverSettings.InitialDatabaseUnavailableMode.FAIL));
	}

	@Test
	@DisruptsMongoService
	void initialOutage_throws() {
		logController.setLogging(ERROR, ChangeReceiver.class);
		mongoService.proxy().setConnectionCut(true);
		tearDownActions.add(()->mongoService.proxy().setConnectionCut(false));
		assertThrows(InitialRootFailureException.class, ()->{
			new Bosk<TestEntity>("Fail", TestEntity.class, this::initialRoot, super.createDriverFactory(logController));
		});
	}

}
