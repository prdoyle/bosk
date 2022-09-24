package io.vena.bosk;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.pcollections.AbstractUnmodifiableMap;
import org.pcollections.HashTreePMap;
import org.pcollections.OrderedPSet;
import org.pcollections.PMap;
import org.pcollections.POrderedSet;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

@Accessors(fluent = true)
@EqualsAndHashCode
@RequiredArgsConstructor(access=AccessLevel.PRIVATE)
public final class SideTable<K extends Entity, V> implements EnumerableByIdentifier<V> {
	@Getter
	private final CatalogReference<K> domain;
	private final PMap<Identifier, V> valuesById;
	private final POrderedSet<Identifier> ids; // Preserves insertion order

	// Note: pcollections is in the process of adding an ordered map.
	// We can adopt that once it's ready.
	// https://github.com/hrldcpr/pcollections/issues/95#issuecomment-1247207869

	public V get(Identifier id) { return valuesById.get(id); }
	public V get(K key)         { return valuesById.get(key.id()); }

	public boolean hasID(Identifier id) { return valuesById.containsKey(id); }
	public boolean hasKey(K key)        { return valuesById.containsKey(key.id()); }

	public boolean isEmpty() { return valuesById.isEmpty(); }
	public int size() { return valuesById.size(); }
	public List<Identifier> ids() { return unmodifiableList(new ArrayList<>(ids)); }
	public Listing<K> keys() { return new Listing<>(domain, ids); }
	public Collection<V> values() { return unmodifiableList(ids.stream().map(valuesById::get).collect(toList())); }

	public Set<Entry<Identifier, V>> idEntrySet() {
		// Type checker needs a little help here
		Supplier<LinkedHashSet<SimpleImmutableEntry<Identifier, V>>> newLinkedHashSet = LinkedHashSet::new;

		return unmodifiableSet(ids.stream()
			.map(id -> new SimpleImmutableEntry<>(id, get(id)))
			.collect(toCollection(newLinkedHashSet))); }

	public Map<Identifier, V> asMap() {
		return new AbstractUnmodifiableMap<Identifier, V>() {
			@Override
			public Set<Entry<Identifier, V>> entrySet() {
				return idEntrySet();
			}
		};
	}

	public Stream<Entry<K, V>> valueEntryStream() {
		AddressableByIdentifier<K> domainValue = domain.value();
		return idEntrySet().stream().map(e -> new SimpleImmutableEntry<>(
			domainValue.get(e.getKey()),
			e.getValue()));
	}

	/**
	 * Note that this requires a read context, and for nonexistent keys,
	 * this will pass null as the key value.
	 *
	 * @see #forEachID
	 */
	public void forEach(BiConsumer<? super K, ? super V> action) {
		AddressableByIdentifier<K> domainValue = domain.value();
		ids.forEach(id -> action.accept(domainValue.get(id), valuesById.get(id)));
	}

	public void forEachID(BiConsumer<Identifier, ? super V> action) {
		ids.forEach(id -> action.accept(id, valuesById.get(id)));
	}

	public SideTable<K,V> with(Identifier id, V value) {
		return new SideTable<>(this.domain, this.valuesById.plus(id, value), this.ids.plus(id));
	}

	public SideTable<K,V> with(K key, V value) {
		return this.with(key.id(), value);
	}

	public SideTable<K,V> updatedWith(Identifier id, Supplier<V> valueIfAbsent, UnaryOperator<V> valueIfPresent) {
		V existing = valuesById.get(id);
		V replacement;
		if (existing == null) {
			replacement = valueIfAbsent.get();
		} else {
			replacement = valueIfPresent.apply(existing);
		}
		return this.with(id, replacement);
	}

	public SideTable<K,V> without(Identifier id) {
		return new SideTable<>(this.domain, valuesById.minus(id), ids.minus(id));
	}

	public SideTable<K,V> without(K key) {
		return this.without(key.id());
	}

	/**
	 * If you get type inference errors with this one, try specifying the value class
	 * with {@link #empty(Reference, Class)}.
	 */
	public static <KK extends Entity,VV> SideTable<KK,VV> empty(Reference<Catalog<KK>> domain) {
		return new SideTable<>(CatalogReference.from(domain), HashTreePMap.empty(), OrderedPSet.empty());
	}

	public static <KK extends Entity,VV> SideTable<KK,VV> empty(Reference<Catalog<KK>> domain, Class<VV> ignored) {
		return empty(domain);
	}

	public static <KK extends Entity, VV> SideTable<KK,VV> of(Reference<Catalog<KK>> domain, Identifier id, VV value) {
		return new SideTable<>(CatalogReference.from(domain), HashTreePMap.singleton(id, value), OrderedPSet.singleton(id));
	}

	public static <KK extends Entity, VV> SideTable<KK,VV> of(Reference<Catalog<KK>> domain, KK key, VV value) {
		return SideTable.of(domain, key.id(), value);
	}

	public static <KK extends Entity,VV> SideTable<KK,VV> fromOrderedMap(Reference<Catalog<KK>> domain, Map<Identifier, VV> contents) {
		return new SideTable<>(CatalogReference.from(domain), HashTreePMap.from(contents), OrderedPSet.from(contents.keySet()));
	}

	public static <KK extends Entity,VV> SideTable<KK,VV> fromFunction(Reference<Catalog<KK>> domain, Stream<Identifier> keyIDs, Function<Identifier, VV> function) {
		LinkedHashMap<Identifier,VV> map = new LinkedHashMap<>();
		keyIDs.forEachOrdered(id -> {
			VV existing = map.put(id, function.apply(id));
			if (existing != null) {
				throw new IllegalArgumentException("Multiple entries with id \"" + id + "\"");
			}
		});
		return new SideTable<>(CatalogReference.from(domain), HashTreePMap.from(map), OrderedPSet.from(map.keySet()));
	}

	@Override
	public String toString() {
		return domain + "/" + valuesById;
	}

}
