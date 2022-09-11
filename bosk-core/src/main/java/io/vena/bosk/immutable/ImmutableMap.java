package io.vena.bosk.immutable;

import java.util.Map;

public interface ImmutableMap<K,V> extends Map<K,V> {
	ImmutableMap<K,V> with(K key, V value);
	ImmutableMap<K,V> without(K key);
}
