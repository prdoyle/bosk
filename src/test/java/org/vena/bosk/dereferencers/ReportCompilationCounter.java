package org.vena.bosk.dereferencers;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

public class ReportCompilationCounter implements BeforeAllCallback {
	@Override
	public void beforeAll(ExtensionContext context) {
		CloseableResource shutdownHook = () ->
			System.err.println("*** Performed " + PathCompiler.numCompilations + " compilations");
		context.getRoot().getStore(GLOBAL).put("compilation counter", shutdownHook);
	}
}
