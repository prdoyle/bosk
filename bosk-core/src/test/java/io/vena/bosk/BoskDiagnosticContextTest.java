package io.vena.bosk;

import io.vena.bosk.annotations.ReferencePath;
import io.vena.bosk.drivers.AbstractDriverTest;
import io.vena.bosk.drivers.state.TestEntity;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.var;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
	void contextPropagatesToHook() {
		AtomicBoolean diagnosticsAreReady = new AtomicBoolean(false);
		bosk.registerHook("contextPropagatesToHook", bosk.rootReference(), ref -> {
			if (diagnosticsAreReady.get()) {
				assertEquals("attributeValue", bosk.diagnosticContext().getAttribute("attributeName"));
				assertEquals(MapValue.singleton("attributeName", "attributeValue"), bosk.diagnosticContext().getAttributes());
			}
		});
		try (var __ = bosk.diagnosticContext().withAttribute("attributeName", "attributeValue")) {
			diagnosticsAreReady.set(true);
			bosk.driver().submitReplacement(refs.string(), "newValue");
			diagnosticsAreReady.set(false);
		}
	}

}
