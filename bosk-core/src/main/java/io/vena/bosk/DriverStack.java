package io.vena.bosk;

import java.util.Arrays;
import java.util.List;

/**
 * Composes multiple {@link DriverFactory} objects into a stack so that they
 * can be instantiated and connected to each other.
 */
public interface DriverStack<R extends StateTreeNode> extends DriverFactory<R> {
	/**
	 * Returns a {@link DriverStack} that composes multiple {@link DriverFactory}
	 * objects in such a way that each factory is downstream of the one before it.
	 * The {@link Bosk}'s local driver will be downstream of the last factory in the list.
	 *
	 * <p>
	 * The arrangement of drivers is such that each operation will be processed by the
	 * driver layers in the given order.
	 *
	 * @param factories in order from upstream to downstream
	 * @return a factory that composes <code>factories</code> from right to left
	 */
	@SafeVarargs
	static <RR extends StateTreeNode> DriverStack<RR> of(DriverFactory<RR>...factories) {
		return DriverStack.of(Arrays.asList(factories));
	}

	static <RR extends StateTreeNode> DriverStack<RR> of(List<DriverFactory<RR>> factories) {
		return new DriverStack<>() {
			@Override
			public BoskDriver<RR> build(BoskInfo<RR> boskInfo, BoskDriver<RR> downstream) {
				BoskDriver<RR> result = downstream;
				for (int i = factories.size() - 1; i >= 0; i--) {
					result = factories.get(i).build(boskInfo, result);
				}
				return result;
			}

			@Override
			public String toString() {
				return factories.toString();
			}
		};
	}
}
