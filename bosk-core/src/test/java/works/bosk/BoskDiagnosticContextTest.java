package works.bosk;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import works.bosk.annotations.ReferencePath;
import works.bosk.drivers.AbstractDriverTest;
import works.bosk.drivers.DriverConformanceTest;
import works.bosk.drivers.state.TestEntity;
import works.bosk.exceptions.InvalidTypeException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Note that context propagation for driver operations is tested by {@link DriverConformanceTest}.
 */
class BoskDiagnosticContextTest extends AbstractDriverTest {
	Refs refs;

	public interface Refs {
		@ReferencePath("/string") Reference<String> string();
	}

	@BeforeEach
	void setupBosk() throws InvalidTypeException {
		bosk = new Bosk<TestEntity>(
			BoskDiagnosticContextTest.class.getSimpleName(),
			TestEntity.class,
			AbstractDriverTest::initialRoot,
			Bosk::simpleDriver
		);
		refs = bosk.buildReferences(Refs.class);
	}

	@Test
	void hookRegistration_propagatesDiagnosticContext() throws IOException, InterruptedException {
		Semaphore diagnosticsVerified = new Semaphore(0);
		bosk.driver().flush();
		try (var __ = bosk.diagnosticContext().withAttribute("attributeName", "attributeValue")) {
			bosk.registerHook("contextPropagatesToHook", bosk.rootReference(), ref -> {
				assertEquals("attributeValue", bosk.diagnosticContext().getAttribute("attributeName"));
				assertEquals(MapValue.singleton("attributeName", "attributeValue"), bosk.diagnosticContext().getAttributes());
				diagnosticsVerified.release();
			});
		}
		bosk.driver().flush();
		assertTrue(diagnosticsVerified.tryAcquire(5, SECONDS));
	}

}
