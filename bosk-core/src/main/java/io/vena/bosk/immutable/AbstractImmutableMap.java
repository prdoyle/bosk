package io.vena.bosk.immutable;

import java.util.AbstractMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public abstract class AbstractImmutableMap<K,V> extends AbstractMap<K,V> implements ImmutableMap<K,V> {
	// Don't forget to re-implement this! The built-in one is O(n)
	@Override public abstract V get(Object key);

	@Override
	public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
		unsupported();
	}

	@Override
	public V putIfAbsent(K key, V value) {
		return unsupported();
	}

	@Override
	public boolean remove(Object key, Object value) {
		return unsupported();
	}

	@Override
	public boolean replace(K key, V oldValue, V newValue) {
		return unsupported();
	}

	@Override
	public V replace(K key, V value) {
		return unsupported();
	}

	@Override
	public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
		return unsupported();
	}

	@Override
	public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
		return unsupported();
	}

	@Override
	public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
		return unsupported();
	}

	@Override
	public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
		return unsupported();
	}

	@Override
	public V put(K key, V value) {
		return unsupported();
	}

	@Override
	public V remove(Object key) {
		return unsupported();
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		unsupported();
	}

	@Override
	public void clear() {
		unsupported();
	}

	private static <T> T unsupported() {
		throw new UnsupportedOperationException();
	}
}
