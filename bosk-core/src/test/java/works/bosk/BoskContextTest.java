package works.bosk;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import works.bosk.annotations.ReferencePath;
import works.bosk.exceptions.InvalidTypeException;
import works.bosk.testing.drivers.AbstractDriverTest;
import works.bosk.testing.drivers.DriverConformanceTest;
import works.bosk.testing.drivers.state.TestEntity;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static works.bosk.logging.MdcKeys.BOSK_INSTANCE_ID;
import static works.bosk.logging.MdcKeys.BOSK_NAME;
import static works.bosk.testing.BoskTestUtils.boskName;

/**
 * Note that context propagation for driver operations is tested by {@link DriverConformanceTest}.
 */
public class BoskContextTest extends AbstractDriverTest {
	public interface Refs {
		@ReferencePath("/string") Reference<String> string();
	}

	@BeforeEach
	void setupBosk() {
		bosk = new Bosk<>(
			boskName(),
			TestEntity.class,
			this::initialState,
			BoskConfig.<TestEntity>builder()
				.build()
		);
	}

	@Test
	void hookRegistration_propagatesContext() throws IOException, InterruptedException {
		Semaphore diagnosticsVerified = new Semaphore(0);
		bosk.driver().flush();
		try (var _ = bosk.context().withAttribute("attributeName", "attributeValue")) {
			bosk.hookRegistrar().registerHook("contextPropagatesToHook", bosk.rootReference(), _ -> {
				assertEquals("attributeValue", bosk.context().getAttribute("attributeName"));
				assertEquals(MapValue.singleton("attributeName", "attributeValue"), bosk.context().getAttributes());
				diagnosticsVerified.release();
			});
		}
		bosk.driver().flush();
		assertTrue(diagnosticsVerified.tryAcquire(5, SECONDS));
	}

	@Test
	void hook_executionEstablishesBoskMdc() throws IOException, InterruptedException {
		Semaphore mdcVerified = new Semaphore(0);
		bosk.driver().flush();
		bosk.hookRegistrar().registerHook("mdcIsSetInHook", bosk.rootReference(), _ -> {
			assertEquals(bosk.name(), MDC.get(BOSK_NAME));
			assertEquals(bosk.instanceID().toString(), MDC.get(BOSK_INSTANCE_ID));
			mdcVerified.release();
		});
		bosk.driver().flush();
		assertTrue(mdcVerified.tryAcquire(5, SECONDS),
			"Hook should see the bosk MDC keys established during its execution");
	}

	@Test
	void hookExecution_seesUpdateContext() throws IOException, InterruptedException, InvalidTypeException {
		Reference<String> stringRef = bosk.rootReference().then(String.class, "string");
		BlockingQueue<MapValue<String>> observed = new LinkedBlockingQueue<>();
		CountDownLatch blockerStarted = new CountDownLatch(1);
		CountDownLatch gate = new CountDownLatch(1);
		AtomicBoolean block = new AtomicBoolean(false);

		bosk.hookRegistrar().registerHook("blocker", bosk.rootReference(), _ -> {
			if (block.compareAndSet(true, false)) {
				blockerStarted.countDown();
				gate.await();
			}
		});
		bosk.hookRegistrar().registerHook("observer", bosk.rootReference(),
			_ -> observed.put(bosk.context().getAttributes()));
		observed.clear();
		block.set(true);

		Thread drainer = new Thread(() -> bosk.driver().submitReplacement(stringRef, "first update"));
		drainer.start();
		assertTrue(blockerStarted.await(10, SECONDS), "The blocker hook must start running");

		// Submit while another thread holds the hook permit, so this update's
		// expansion happens later, on the drainer thread.
		MapValue<String> updateAttributes = MapValue.singleton("updateAttribute", "updateValue");
		try (var _ = bosk.context().withAttributes(updateAttributes)) {
			bosk.driver().submitReplacement(stringRef, "second update");
		}
		gate.countDown();
		drainer.join(10_000);
		assertFalse(drainer.isAlive(), "The drainer must finish");

		assertEquals(MapValue.empty(), observed.poll(5, SECONDS),
			"The first update's hook sees no diagnostic attributes");
		assertEquals(updateAttributes, observed.poll(5, SECONDS),
			"The hook should see the attributes of the update that triggered it, even though another thread expands it");
	}

	@Test
	void replacePrefix_works() {
		MapValue<String> expectedOuter = MapValue.copyOf(Map.of(
			"unprefixed", "unprefixedValue",
			"prefix.key1", "outer1",
			"prefix.key2", "outer2"
		));
		MapValue<String> overrides = MapValue.copyOf(Map.of(
			"key1", "inner1",
			"key3", "inner3"
		));
		MapValue<String> expectedInner = MapValue.copyOf(Map.of(
			"unprefixed", "unprefixedValue",
			"prefix.key1", "inner1",
			"prefix.key3", "inner3"
		));
		var context = bosk.context();
		try (var _ = context.withAttributes(expectedOuter)) {
			assertEquals(expectedOuter, context.getAttributes());
			try (var _ = context.withReplacedPrefix("prefix.", overrides)) {
				assertEquals(expectedInner, context.getAttributes());
			}
		}
	}

	@Test
	void wrongOrder_throws() {
		var context = bosk.context();
		try (
			var scope1 = context.withAttribute("key", "scope1");
			var _ = context.withAttribute("key", "scope2")
		) {
			assertThrows(IllegalStateException.class, scope1::close,
				"Closing ContextScopes in the wrong order should throw");
		}

		// (If we made it to this point, we were able to close the scopes
		// in the correct order even after trying to close them in the wrong order.)
	}

}
