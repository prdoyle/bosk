package io.vena.bosk.immutable;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

import static java.util.Collections.emptyIterator;

class EmptyNode<K, V> implements TreeNode<K,V> {
	private static final TreeNode<?,?> INSTANCE = new EmptyNode<>();

	@SuppressWarnings("unchecked")
	public static <KK,VV> EmptyNode<KK,VV> instance() {
		return (EmptyNode<KK, VV>) INSTANCE;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public TreeNode<K, V> with(K key, V value, Comparator<K> comparator) {
		return new OccupiedNode<>(key, value, this, this);
	}

	@Override
	public TreeNode<K, V> without(K key, Comparator<K> comparator) {
		return this;
	}

	@Override
	public TreeNode<K, V> withAll(TreeNode<K, V> other, Comparator<K> comparator) {
		return other;
	}

	@Override
	public TreeNode<K, V> difference(TreeNode<K, V> other, Comparator<K> comparator) {
		return this;
	}

	@Override
	public TreeNode<K, V> intersection(TreeNode<K, V> other, Comparator<K> comparator) {
		return this;
	}

	@Override
	public Iterator<Map.Entry<K, V>> entryIterator() {
		return emptyIterator();
	}

	@Override
	public String toString() {
		return "EmptyNode";
	}
}
