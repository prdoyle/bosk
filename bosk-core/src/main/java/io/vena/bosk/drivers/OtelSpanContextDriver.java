package io.vena.bosk.drivers;

import io.opentelemetry.api.trace.SpanContext;
import io.vena.bosk.BoskDiagnosticContext;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.DriverFactory;
import io.vena.bosk.Identifier;
import io.vena.bosk.Reference;
import io.vena.bosk.StateTreeNode;
import io.vena.bosk.exceptions.InvalidTypeException;
import java.io.IOException;
import java.lang.reflect.Type;

/**
 * Adds OpenTelemetry {@link SpanContext} info to the {@link BoskDiagnosticContext}
 * automatically on each method call.
 */
public class OtelSpanContextDriver<R extends StateTreeNode> implements BoskDriver<R> {
	private final BoskDiagnosticContext context;
	private final BoskDriver<R> downstream;

	OtelSpanContextDriver(BoskDiagnosticContext context, BoskDriver<R> downstream) {
		this.context = context;
		this.downstream = downstream;
	}

	public static <RR extends StateTreeNode> DriverFactory<RR> factory() {
		return (b,d) -> new OtelSpanContextDriver<>(b.rootReference().diagnosticContext(), d);
	}

	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		try (var __ = context.withCurrentOtelContext()) {
			return downstream.initialRoot(rootType);
		}
	}

	@Override
	public <T> void submitReplacement(Reference<T> target, T newValue) {
		try (var __ = context.withCurrentOtelContext()) {
			downstream.submitReplacement(target, newValue);
		}
	}

	@Override
	public <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		try (var __ = context.withCurrentOtelContext()) {
			downstream.submitConditionalReplacement(target, newValue, precondition, requiredValue);
		}
	}

	@Override
	public <T> void submitInitialization(Reference<T> target, T newValue) {
		try (var __ = context.withCurrentOtelContext()) {
			downstream.submitInitialization(target, newValue);
		}
	}

	@Override
	public <T> void submitDeletion(Reference<T> target) {
		try (var __ = context.withCurrentOtelContext()) {
			downstream.submitDeletion(target);
		}
	}

	@Override
	public <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		try (var __ = context.withCurrentOtelContext()) {
			downstream.submitConditionalDeletion(target, precondition, requiredValue);
		}
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		try (var __ = context.withCurrentOtelContext()) {
			downstream.flush();
		}
	}
}
