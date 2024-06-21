package io.vena.bosk.drivers;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;
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
	private final String caller;

	OtelSpanContextDriver(BoskDiagnosticContext context, BoskDriver<R> downstream, String caller) {
		this.context = context;
		this.downstream = downstream;
		this.caller = caller;
	}

	public static <RR extends StateTreeNode> DriverFactory<RR> factory() {
		StackTraceElement caller = new RuntimeException().getStackTrace()[1];
		return (b,d) -> new OtelSpanContextDriver<>(
			b.rootReference().diagnosticContext(),
			d,
			caller.toString()
		);
	}

	@Override
	@WithSpan("initialRoot")
	public R initialRoot(
		@SpanAttribute("bosk.rootType") Type rootType
	) throws InvalidTypeException, IOException, InterruptedException {
		Span.current().setAttribute("bosk.createdAt", caller);
		try (var __ = context.withCurrentOtelContext()) {
			return downstream.initialRoot(rootType);
		}
	}

	@Override
	@WithSpan("submitReplacement")
	public <T> void submitReplacement(
		@SpanAttribute("bosk.target") Reference<T> target,
		T newValue
	) {
		Span.current().setAttribute("bosk.createdAt", caller);
		try (var __ = context.withCurrentOtelContext()) {
			downstream.submitReplacement(target, newValue);
		}
	}

	@Override
	@WithSpan("submitConditionalReplacement")
	public <T> void submitConditionalReplacement(
		@SpanAttribute("bosk.target") Reference<T> target,
		T newValue,
		@SpanAttribute("bosk.precondition") Reference<Identifier> precondition,
		@SpanAttribute("bosk.requiredValue") Identifier requiredValue
	) {
		Span.current().setAttribute("bosk.createdAt", caller);
		try (var __ = context.withCurrentOtelContext()) {
			downstream.submitConditionalReplacement(target, newValue, precondition, requiredValue);
		}
	}

	@Override
	@WithSpan("submitInitialization")
	public <T> void submitInitialization(
		@SpanAttribute("bosk.target") Reference<T> target,
		T newValue
	) {
		Span.current().setAttribute("bosk.createdAt", caller);
		try (var __ = context.withCurrentOtelContext()) {
			downstream.submitInitialization(target, newValue);
		}
	}

	@Override
	@WithSpan("submitDeletion")
	public <T> void submitDeletion(
		@SpanAttribute("bosk.target") Reference<T> target
	) {
		Span.current().setAttribute("bosk.createdAt", caller);
		try (var __ = context.withCurrentOtelContext()) {
			downstream.submitDeletion(target);
		}
	}

	@Override
	@WithSpan("submitConditionalDeletion")
	public <T> void submitConditionalDeletion(
		@SpanAttribute("bosk.target") Reference<T> target,
		@SpanAttribute("bosk.precondition") Reference<Identifier> precondition,
		@SpanAttribute("bosk.requiredValue") Identifier requiredValue
	) {
		Span.current().setAttribute("bosk.createdAt", caller);
		try (var __ = context.withCurrentOtelContext()) {
			downstream.submitConditionalDeletion(target, precondition, requiredValue);
		}
	}

	@Override
	@WithSpan("InterruptedException")
	public void flush() throws IOException, InterruptedException {
		Span.current().setAttribute("bosk.createdAt", caller);
		try (var __ = context.withCurrentOtelContext()) {
			downstream.flush();
		}
	}
}
