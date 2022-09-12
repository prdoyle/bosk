package io.vena.bosk.immutable;

import java.util.AbstractList;
import java.util.Collection;
import java.util.Comparator;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public abstract class AbstractImmutableList<E> extends AbstractList<E> implements ImmutableList<E> {
	@Override
	public boolean add(E e) {
		return unsupported();
	}

	@Override
	public E set(int index, E element) {
		return unsupported();
	}

	@Override
	public void add(int index, E element) {
		unsupported();
	}

	@Override
	public E remove(int index) {
		return unsupported();
	}

	@Override
	public void clear() {
		unsupported();
	}

	@Override
	public boolean addAll(int index, Collection<? extends E> c) {
		return unsupported();
	}

	@Override
	protected void removeRange(int fromIndex, int toIndex) {
		unsupported();
	}

	@Override
	public boolean remove(Object o) {
		return unsupported();
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		return unsupported();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		return unsupported();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		return unsupported();
	}

	@Override
	public void replaceAll(UnaryOperator<E> operator) {
		unsupported();
	}

	@Override
	public void sort(Comparator<? super E> c) {
		unsupported();
	}

	@Override
	public boolean removeIf(Predicate<? super E> filter) {
		return unsupported();
	}

	private static <T> T unsupported() {
		throw new UnsupportedOperationException();
	}
}
