package io.vena.bosk.drivers.mongo;

import com.mongodb.MongoClientSettings;
import io.vena.bosk.AbstractBoskTest;
import io.vena.bosk.Bosk;
import io.vena.bosk.exceptions.FlushFailureException;
import java.io.IOException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MongoStandaloneTest extends AbstractBoskTest {
	private static final MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:4.4"));

	@BeforeAll
	static void startContainer() {
		mongoDBContainer.start();
	}

	@Test
	void standaloneServer_reportedCorrectly() throws IOException, InterruptedException {
		int connectTimeout = 10_000;
		int readTimeout = 2000;
		int recoveryPollingMS = 2000;
		Bosk<TestRoot> bosk = setUpBosk(
			MongoDriver.factory(
				MongoClientSettings.builder()
					.applyToClusterSettings(builder -> {
						builder.serverSelectionTimeout(connectTimeout, MILLISECONDS);
					})
					.applyToSocketSettings(builder -> {
						builder.connectTimeout(connectTimeout, MILLISECONDS);
						builder.readTimeout(readTimeout, MILLISECONDS);
					})
					.build(),
				MongoDriverSettings.builder()
					.database(MongoStandaloneTest.class.getSimpleName())
					.recoveryPollingMS(recoveryPollingMS)
					.build(),
				new BsonPlugin()));
		assertThrows(FlushFailureException.class, ()->bosk.driver().flush());
	}
}
