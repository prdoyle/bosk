package io.vena.bosk.immutable;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;

import static lombok.AccessLevel.PRIVATE;

@RequiredArgsConstructor(access = PRIVATE)
public class ImmutableTreeMap<K extends Comparable<K>, V> extends AbstractImmutableMap<K,V> {
	private final TreeNode<K, V> rootNode;

	@SuppressWarnings("unchecked")
	public static <KK extends Comparable<KK>,VV> ImmutableTreeMap<KK,VV> empty() {
		return (ImmutableTreeMap<KK, VV>) EMPTY;
	}

	private static final ImmutableTreeMap<?,?> EMPTY = new ImmutableTreeMap<>(TreeNode.empty());

	@Override
	public ImmutableTreeMap<K, V> with(K key, V value) {
		return new ImmutableTreeMap<>(rootNode.with(key, value, Comparable::compareTo));
	}

	@Override
	public ImmutableTreeMap<K, V> withAll(Iterator<Map.Entry<K, V>> newEntries) {
		TreeNode<K, V> newRoot = this.rootNode;
		Iterable<Map.Entry<K,V>> iter = ()->newEntries;
		for (Map.Entry<K, V> entry: iter) {
			newRoot = newRoot.with(entry.getKey(), entry.getValue(), Comparable::compareTo);
		}
		return new ImmutableTreeMap<>(newRoot);
	}


	@Override
	public ImmutableTreeMap<K, V> without(K key) {
		return new ImmutableTreeMap<>(rootNode.without(key, Comparable::compareTo));
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		return new AbstractSet<Entry<K, V>>() {
			@Override
			public Iterator<Entry<K, V>> iterator() {
				return rootNode.entryIterator();
			}

			@Override
			public int size() {
				return rootNode.size();
			}
		};
	}

	@Override
	@SuppressWarnings("unchecked")
	public V get(Object key) {
		return rootNode.get((K)key, Comparable::compareTo);
	}
}
