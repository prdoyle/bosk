package works.bosk;

import java.util.Objects;

/**
 * A {@link StateTreeNode} representing one of several possible {@link VariantCase}s.
 */
public record TaggedUnion<V extends VariantCase>(V variant) {
	public TaggedUnion {
		Objects.requireNonNull(variant);
	}

	public static <VV extends VariantCase> TaggedUnion<VV> of (VV variant) {
		return new TaggedUnion<>(variant);
	}
}
