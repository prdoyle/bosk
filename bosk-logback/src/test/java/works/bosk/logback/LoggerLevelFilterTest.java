package works.bosk.logback;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.spi.FilterReply;
import org.junit.jupiter.api.Test;
import works.bosk.logback.LoggerLevelFilter.LoggerConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LoggerLevelFilterTest {

	@Test
	void mongoDebugIsDeniedButInfoAllowed() {
		LoggerLevelFilter filter = new LoggerLevelFilter();

		LoggerConfig r1 = new LoggerConfig();
		r1.setName("com.mongodb");
		r1.setLevel("INFO");
		filter.addLogger(r1);
		filter.start();

		Logger logger = new ch.qos.logback.classic.LoggerContext().getLogger("com.mongodb.Driver");

		ILoggingEvent debugEvent = new LoggingEvent(logger.getName(), logger, Level.DEBUG, "m", null, null);
		assertEquals(FilterReply.DENY, filter.decide(debugEvent));

		ILoggingEvent infoEvent = new LoggingEvent(logger.getName(), logger, Level.INFO, "m", null, null);
		assertEquals(FilterReply.NEUTRAL, filter.decide(infoEvent));
	}

	@Test
	void specificLoggerOverridesParent() {
		LoggerLevelFilter filter = new LoggerLevelFilter();

		LoggerConfig parent = new LoggerConfig();
		parent.setName("works.bosk");
		parent.setLevel("INFO");
		filter.addLogger(parent);

		LoggerConfig child = new LoggerConfig();
		child.setName("works.bosk.boson");
		child.setLevel("DEBUG");
		filter.addLogger(child);

		filter.start();

		Logger parentLogger = new ch.qos.logback.classic.LoggerContext().getLogger("works.bosk.Other");
		ILoggingEvent parentDebug = new LoggingEvent(parentLogger.getName(), parentLogger, Level.DEBUG, "m", null, null);
		assertEquals(FilterReply.DENY, filter.decide(parentDebug));

		Logger childLogger = new ch.qos.logback.classic.LoggerContext().getLogger("works.bosk.boson.SomeClass");
		ILoggingEvent childDebug = new LoggingEvent(childLogger.getName(), childLogger, Level.DEBUG, "m", null, null);
		assertEquals(FilterReply.NEUTRAL, filter.decide(childDebug));
	}
}
