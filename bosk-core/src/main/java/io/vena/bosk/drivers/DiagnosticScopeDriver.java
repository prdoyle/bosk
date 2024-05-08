package io.vena.bosk.drivers;

import io.vena.bosk.BoskDiagnosticContext;
import io.vena.bosk.BoskDiagnosticContext.DiagnosticScope;
import io.vena.bosk.BoskDriver;
import io.vena.bosk.DriverFactory;
import io.vena.bosk.StateTreeNode;
import io.vena.bosk.exceptions.InvalidTypeException;
import io.vena.bosk.updates.Update;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

import static lombok.AccessLevel.PRIVATE;

/**
 * Automatically sets a {@link DiagnosticScope} around each driver operation based on a user-supplied function.
 * Allows diagnostic context to be supplied automatically to every operation.
 */
@RequiredArgsConstructor(access = PRIVATE)
public class DiagnosticScopeDriver<R extends StateTreeNode> implements BoskDriver<R> {
	final BoskDriver<R> downstream;
	final BoskDiagnosticContext diagnosticContext;
	final Function<BoskDiagnosticContext, DiagnosticScope> scopeSupplier;

	public static <RR extends StateTreeNode> DriverFactory<RR> factory(Function<BoskDiagnosticContext, DiagnosticScope> scopeSupplier) {
		return (b,d) -> new DiagnosticScopeDriver<>(d, b.rootReference().diagnosticContext(), scopeSupplier);
	}

	@Override
	public R initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		try (var __ = scopeSupplier.apply(diagnosticContext)) {
			return downstream.initialRoot(rootType);
		}
	}

	@Override
	public <T> void submit(Update<T> update) {
		try (var __ = scopeSupplier.apply(diagnosticContext)) {
			downstream.submit(update);
		}
	}

	@Override
	public void flush() throws IOException, InterruptedException {
		try (var __ = scopeSupplier.apply(diagnosticContext)) {
			downstream.flush();
		}
	}
}
