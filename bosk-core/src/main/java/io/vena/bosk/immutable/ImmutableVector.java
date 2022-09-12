package io.vena.bosk.immutable;

import io.vena.bosk.exceptions.NotYetImplementedException;
import java.util.Comparator;
import lombok.RequiredArgsConstructor;

import static lombok.AccessLevel.PRIVATE;

@RequiredArgsConstructor(access = PRIVATE)
public final class ImmutableVector<E> extends AbstractImmutableList<E> {
	final TreeNode<Integer, E> root;

	@Override
	public ImmutableList<E> with(E element) {
		return new ImmutableVector<>(root.with(root.size(), element, INTEGER_COMPARATOR));
	}

	@Override
	public ImmutableList<E> without(int index) {
		return new ImmutableVector<>(root.without(index, INTEGER_COMPARATOR));
	}

	@Override
	public E get(int index) {
		return root.get(index, INTEGER_COMPARATOR);
	}

	@Override
	public int size() {
		return root.size();
	}

	static final Comparator<Integer> INTEGER_COMPARATOR = Integer::compareTo;
}
