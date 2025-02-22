package works.bosk;

/**
 * An object that can participate in a {@link TaggedUnion}
 * via a {@link works.bosk.annotations.VariantCaseMap @VariantCaseMap}.
 */
public interface VariantCase extends StateTreeNode {
	String tag(); // TODO: Should be an Identifier
}
