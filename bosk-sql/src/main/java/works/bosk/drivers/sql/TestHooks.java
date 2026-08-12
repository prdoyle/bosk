package works.bosk.drivers.sql;

import lombok.With;

/**
 * Test hooks: the test-only facilities that {@link SqlDriverImpl} consults at
 * well-defined points, so tests can deterministically coordinate with the
 * driver's internals.
 * <p>
 * All hooks are no-ops by default; tests install the hooks they need by
 * starting from {@link #noop()} and using the {@code with} methods. The hooks
 * are read from {@link SqlDriverImpl#TEST_HOOKS} on the thread that constructs
 * the {@code SqlDriverImpl}, and are captured at construction time, so they
 * apply to every thread that later does database work.
 * <p>
 * The hooks are:
 * <ul>
 * <li>{@code afterStateRead}: runs after {@code SqlDriverImpl} reads the state
 * row inside a submit operation, before it writes back. Tests can use it to
 * block a thread between its read and its write, keeping the database
 * transaction open, to force a specific interleaving of concurrent
 * submissions.</li>
 * </ul>
 */
@With
record TestHooks(
	Runnable afterStateRead
) {
	static TestHooks noop() {
		return new TestHooks(NOOP);
	}

	private static final Runnable NOOP = () -> {};
}
