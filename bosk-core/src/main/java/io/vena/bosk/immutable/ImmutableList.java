package io.vena.bosk.immutable;

import java.util.List;

public interface ImmutableList<E> extends List<E> {
	ImmutableList<E> with(E element);
	ImmutableList<E> without(int index);
}
