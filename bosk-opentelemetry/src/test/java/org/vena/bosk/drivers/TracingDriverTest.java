package org.vena.bosk.drivers;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.sdk.testing.assertj.TraceAssert;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.StatusData;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.vena.bosk.Bosk;
import org.vena.bosk.BoskDriver;
import org.vena.bosk.Identifier;
import org.vena.bosk.drivers.state.TestEntity;
import org.vena.bosk.drivers.state.TestValues;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static io.opentelemetry.api.trace.SpanKind.INTERNAL;
import static io.opentelemetry.sdk.trace.data.StatusData.ok;
import static io.opentelemetry.sdk.trace.data.StatusData.unset;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.vena.bosk.drivers.TracingDriver.ATTR_OPERATION;
import static org.vena.bosk.drivers.TracingDriver.ATTR_PRECONDITION_PATH;
import static org.vena.bosk.drivers.TracingDriver.ATTR_PRECONDITION_REQ;
import static org.vena.bosk.drivers.TracingDriver.ATTR_TARGET_CLASS;
import static org.vena.bosk.drivers.TracingDriver.ATTR_TARGET_PATH;
import static org.vena.bosk.drivers.TracingDriver.ATTR_TARGET_TYPE;

public class TracingDriverTest extends AbstractDriverTest {
	@RegisterExtension
	static final OpenTelemetryExtension OTEL = OpenTelemetryExtension.create();

	TracerProvider tracerProvider;
	private Attributes customAttributes;

	@BeforeEach
	void setup() {
		// Prevent any extra Bosk tracing from messing up the results
		Bosk.setTracerFrom(TracerProvider.noop());

		tracerProvider = OTEL.getOpenTelemetry().getTracerProvider();
		customAttributes = Attributes.of(KEY_1, "value1");
	}

	@Test
	void invalidCustomAttribute_throws() {
		assertThrows(IllegalArgumentException.class, () ->
			setupBosksAndReferences((downstream, bosk)->
				new TracingDriver<>(
					tracerProvider,
					Attributes.of(stringKey("bosk.bogus"), "x"),
					downstream)));
	}

	@Test
	void initialRoot_okSpan() {
		setupBosksAndReferences((downstream, bosk) ->
			new TracingDriver<>(tracerProvider, customAttributes, downstream));
		OTEL.assertTraces().hasTracesSatisfyingExactly(
			isInitialRootTrace(ok())
		);
	}

	@Test
	void initialRoot_exceptionSpan() {
		assertThrows(IllegalStateException.class, () ->
			setupBosksAndReferences((downstream, bosk) ->
				new TracingDriver<>(tracerProvider, customAttributes,
					new PrePostDriver<>(()->{ throw new IllegalStateException(); }, ()->{},
						downstream)))
		);
		OTEL.assertTraces().hasTracesSatisfyingExactly(
			isInitialRootTrace(unset())
		);
	}

	@ParameterizedTest
	@MethodSource("operations")
	void completes_okSpan(String operationName, Attributes operationSpecificAttributes, Operation operation) throws Exception {
		setupBosksAndReferences((downstream, bosk) ->
			new TracingDriver<>(tracerProvider, customAttributes, downstream));

		Attributes expectedAttributes = operationSpecificAttributes.toBuilder()
			.put(ATTR_OPERATION, operationName)
			.put(KEY_1, "value1")
			.build();

		operation.accept(bosk);

		OTEL.assertTraces().hasTracesSatisfyingExactly(
			isInitialRootTrace(ok()),
			trace->trace.hasSpansSatisfyingExactly(
				span -> span
					.hasName("BoskDriver." + operationName)
					.hasKind(INTERNAL)
					.hasStatus(ok())
					.hasAttributes(expectedAttributes)
			)
		);
	}

	@ParameterizedTest
	@MethodSource("operations")
	void operationThrows_exceptionSpan(String operationName, Attributes operationSpecificAttributes, Operation operation) throws Exception {
		AtomicBoolean shouldThrow = new AtomicBoolean(false);
		String message = "The exception message";

		setupBosksAndReferences((downstream, bosk) ->
			new TracingDriver<>(tracerProvider, customAttributes,
				new PrePostDriver<>(()->{ if (shouldThrow.get()) throw new IllegalStateException(message); }, ()->{},
					downstream)));

		Attributes expectedAttributes = operationSpecificAttributes.toBuilder()
			.put(ATTR_OPERATION, operationName)
			.put(KEY_1, "value1")
			.build();

		shouldThrow.set(true);
		assertThrows(IllegalStateException.class, () -> operation.accept(bosk));

		OTEL.assertTraces().hasTracesSatisfyingExactly(
			isInitialRootTrace(ok()),
			trace->trace.hasSpansSatisfyingExactly(
				span -> span
					.hasName("BoskDriver." + operationName)
					.hasKind(INTERNAL)
					.hasStatus(unset())
					.hasAttributes(expectedAttributes)
					.hasException(new IllegalStateException(message))
			)
		);
	}

	/**
	 * @return one of each kind of {@link BoskDriver} operation
	 */
	public static Stream<Arguments> operations() {
		return Stream.of(
			operation("submitReplacement",
				Attributes.of(
					ATTR_TARGET_PATH, "/",
					ATTR_TARGET_CLASS, TestEntity.class.getName(),
					ATTR_TARGET_TYPE, TestEntity.class.getTypeName()
				),
				bosk -> bosk.driver().submitReplacement(
					bosk.rootReference(), initialRoot(bosk)
				)
			),
			operation("submitConditionalReplacement",
				Attributes.of(
					ATTR_TARGET_PATH, "/",
					ATTR_TARGET_CLASS, TestEntity.class.getName(),
					ATTR_TARGET_TYPE, TestEntity.class.getTypeName(),
					ATTR_PRECONDITION_PATH, "/id",
					ATTR_PRECONDITION_REQ, "precondition"

				),
				bosk -> bosk.driver().submitConditionalReplacement(
					bosk.rootReference(), initialRoot(bosk),
					bosk.rootReference().then(Identifier.class, "id"), Identifier.from("precondition")
				)
			),
			operation("submitInitialization",
				Attributes.of(
					ATTR_TARGET_PATH, "/",
					ATTR_TARGET_CLASS, TestEntity.class.getName(),
					ATTR_TARGET_TYPE, TestEntity.class.getTypeName()
				),
				bosk -> bosk.driver().submitInitialization(
					bosk.rootReference(), initialRoot(bosk)
				)
			),
			operation("submitDeletion",
				Attributes.of(
					ATTR_TARGET_PATH, "/values",
					ATTR_TARGET_CLASS, TestValues.class.getName(),
					ATTR_TARGET_TYPE, TestValues.class.getTypeName()
				),
				bosk -> bosk.driver().submitDeletion(
					bosk.rootReference().then(TestValues.class, TestEntity.Fields.values)
				)
			),
			operation("submitConditionalDeletion",
				Attributes.of(
					ATTR_TARGET_PATH, "/values",
					ATTR_TARGET_CLASS, TestValues.class.getName(),
					ATTR_TARGET_TYPE, TestValues.class.getTypeName(),
					ATTR_PRECONDITION_PATH, "/id",
					ATTR_PRECONDITION_REQ, "precondition"
				),
				bosk -> bosk.driver().submitConditionalDeletion(
					bosk.rootReference().then(TestValues.class, TestEntity.Fields.values),
					bosk.rootReference().then(Identifier.class, "id"), Identifier.from("precondition")
				)
			),
			operation("flush",
				Attributes.empty(),
				bosk -> bosk.driver().flush()
			)
		);
	}

	public static Arguments operation(String operationName, Attributes expectedAttributes, Operation operation) {
		return Arguments.of(operationName, expectedAttributes, operation);
	}

	static Attributes standardAttributes() {
		return Attributes.of(
			TracingDriver.ATTR_ROOT_CLASS, TestEntity.class.getName(),
			TracingDriver.ATTR_ROOT_TYPE,  TestEntity.class.getTypeName()
		);
	}

	static final AttributeKey<String> KEY_1 = stringKey("key1");

	static Consumer<TraceAssert> isInitialRootTrace(StatusData status) {
		return trace -> trace.hasSpansSatisfyingExactly(
			span->span
				.hasName("BoskDriver.initialRoot")
				.hasKind(INTERNAL)
				.hasStatus(status)
				.hasAttributes(standardAttributes().toBuilder()
					.put(ATTR_OPERATION, "initialRoot")
					.put(KEY_1, "value1")
					.build())
		);
	}

	interface Operation {
		void accept(Bosk<TestEntity> bosk) throws Exception;
	}
}
