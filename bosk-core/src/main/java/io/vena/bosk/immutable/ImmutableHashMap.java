package io.vena.bosk.immutable;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.Accessors;

import static io.vena.bosk.immutable.ImmutableVector.INTEGER_COMPARATOR;
import static java.util.Collections.emptyIterator;
import static lombok.AccessLevel.PRIVATE;

@RequiredArgsConstructor(access = PRIVATE)
public final class ImmutableHashMap<K,V> extends AbstractImmutableMap<K,V> {
	private final TreeNode<Integer, ImmutableList<Entry<K,V>>> rootNode;

	public static <K,V> ImmutableMap<K,V> empty()  {
		return new ImmutableHashMap<>(TreeNode.empty());
	}

	@Override
	public ImmutableMap<K, V> with(K key, V value) {
		int hash = key.hashCode();
		ImmutableList<Entry<K,V>> bucket = rootNode.get(hash, INTEGER_COMPARATOR);
		return new ImmutableHashMap<>(rootNode.with(hash, bucket.with(new Entry<>(key, value)), INTEGER_COMPARATOR));
	}

	@Override
	public ImmutableMap<K, V> without(K key) {
		int hash = key.hashCode();
		ImmutableList<Entry<K,V>> bucket = rootNode.get(hash, INTEGER_COMPARATOR);
		return new ImmutableHashMap<>(rootNode.with(hash, bucket.without(bucket.indexOf(new Entry<K,V>(key, null))), INTEGER_COMPARATOR));
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		return new AbstractSet<Map.Entry<K, V>>() {
			final Iterator<Map.Entry<Integer, ImmutableList<Entry<K, V>>>> entryIter = rootNode.entryIterator();
			Iterator<Entry<K, V>> vIter = emptyIterator();

			@Override
			public Iterator<Map.Entry<K, V>> iterator() {
				return new Iterator<Map.Entry<K, V>>() {
					@Override
					public boolean hasNext() {
						return entryIter.hasNext() || vIter.hasNext();
					}

					@Override
					public Map.Entry<K, V> next() {
						if (!vIter.hasNext()) {
							vIter = entryIter.next().getValue().iterator();
						}
						Entry<K, V> next = vIter.next();
						return new SimpleEntry<>(next.key, next.value);
					}
				};
			}

			@Override
			public int size() {
				return rootNode.size();
			}
		};
	}

	@Value
	@Accessors(fluent = true)
	private static class Entry<KK,VV> {
		KK key;
		@EqualsAndHashCode.Exclude VV value;
	}
}
