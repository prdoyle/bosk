package io.vena.bosk.drivers.mongo.v3;

import org.slf4j.MDC;

final class MappedDiagnosticContext {
	static MDCScope setupMDC(String boskName) {
		MDCScope result = new MDCScope();
		MDC.put(MDC_KEY, " [" + boskName + "]");
		return result;
	}

	/**
	 * This is like {@link org.slf4j.MDC.MDCCloseable} except instead of
	 * deleting the MDC entry at the end, it restores it to its prior value,
	 * which allows us to nest these.
	 *
	 * <p>
	 * Note that for a try block using one of these, the catch and finally
	 * blocks will run after {@link #close()} and won't have the context.
	 * You probably want to use this in a try block with no catch or finally clause.
	 */
	static final class MDCScope implements AutoCloseable {
		final String oldValue = MDC.get(MDC_KEY);
		@Override public void close() { MDC.put(MDC_KEY, oldValue); }
	}

	private static final String MDC_KEY = "MongoDriver";
}
