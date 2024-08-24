package works.bosk;

/**
 * @param <R> permits us to ensure at compile time that the driver factories
 *           for a given bosk are all prepared to use the same root type.
 */
public interface DriverFactory<R extends StateTreeNode> {
	BoskDriver build(BoskInfo<R> boskInfo, BoskDriver downstream);
}
