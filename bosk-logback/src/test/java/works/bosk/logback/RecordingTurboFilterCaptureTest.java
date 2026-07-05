package works.bosk.logback;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.helpers.SubstituteLoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RecordingTurboFilterCaptureTest {
	private RecordingTurboFilter filter;

	@BeforeAll
	static void initializeLogging() throws InterruptedException {
		// Logback offers no known way to wait for initialization to finish before running tests!
		while (LoggerFactory.getILoggerFactory() instanceof SubstituteLoggerFactory) {
			Thread.sleep(100);
		}
	}

	@BeforeEach
	void setup() {
		filter = new RecordingTurboFilter();
		filter.setEnabled(true);
		filter.setLevel(Level.DEBUG);
		filter.setCapacity(10);

		// Deny MongoDB DEBUG/INFO events from being buffered
		filter.setFilter(new Filter<>() {
			@Override
			public FilterReply decide(ILoggingEvent event) {
				String name = event.getLoggerName();
				if (name != null && (name.startsWith("com.mongodb") || name.startsWith("org.mongodb"))
					&& event.getLevel().toInt() <= Level.INFO_INT) {
					return FilterReply.DENY;
				}
				return FilterReply.NEUTRAL;
			}

			@Override
			public void start() {
			}

			@Override
			public void stop() {
			}
		});

		filter.start();
	}

	@AfterEach
	void tearDown() {
		filter.stop();
		MDC.remove(RecordingTurboFilter.TEST_ID_KEY);
	}

	@Test
	void mongoDebugNotBuffered() {
		MDC.put(RecordingTurboFilter.TEST_ID_KEY, "t1");
		Logger logger = new ch.qos.logback.classic.LoggerContext().getLogger("com.mongodb.Driver");
		filter.decide(null, logger, Level.DEBUG, "m", null, null);
		RecordingTurboFilter.QueueContents qc = filter.queueContents("t1");
		assertEquals(0, qc.events().size());
	}

	@Test
	void otherLoggerBuffered() {
		MDC.put(RecordingTurboFilter.TEST_ID_KEY, "t2");
		Logger logger = new ch.qos.logback.classic.LoggerContext().getLogger("works.bosk.Some");
		filter.decide(null, logger, Level.DEBUG, "m", null, null);
		RecordingTurboFilter.QueueContents qc = filter.queueContents("t2");
		assertEquals(1, qc.events().size());
	}
}
