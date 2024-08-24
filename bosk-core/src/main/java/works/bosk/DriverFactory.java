package works.bosk;

public interface DriverFactory<R extends StateTreeNode> {
	BoskDriver build(BoskInfo<R> boskInfo, BoskDriver downstream);
}
