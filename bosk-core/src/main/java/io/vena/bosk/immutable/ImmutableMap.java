package io.vena.bosk.immutable;

import java.util.Iterator;
import java.util.Map;

public interface ImmutableMap<K,V> extends Map<K,V> {
	ImmutableMap<K,V> with(K key, V value);
	ImmutableMap<K,V> withAll(Iterator<Map.Entry<K,V>> newEntries);
	ImmutableMap<K,V> without(K key);
}
