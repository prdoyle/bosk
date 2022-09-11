package io.vena.bosk.immutable;

import java.util.Set;

public class ImmutableHashMap<K,V> extends AbstractImmutableMap<K,V> {
	@Override
	public ImmutableMap<K, V> with(K key, V value) {
		return null;
	}

	@Override
	public ImmutableMap<K, V> without(K key) {
		return null;
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		return null;
	}
}
