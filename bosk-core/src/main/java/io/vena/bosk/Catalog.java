package io.vena.bosk;

import io.vena.bosk.immutable.ImmutableTreeMap;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static lombok.AccessLevel.PROTECTED;

/**
 * An ordered collection of entities included by value. Mainly useful to represent
 * one-to-many containment relationships in the Bosk state tree, but also occasionally
 * handy as a data structure on its own.
 *
 * <p>
 * Behaves like a {@link LinkedHashMap}, except immutable, and we automatically
 * know the key for each entry: its {@link Entity#id}.
 *
 * <p>
 * Because a <code>Catalog</code> <em>contains</em> its entries, a {@link io.vena.bosk.Bosk.ReadContext}
 * is not required to access them.
 *
 * @author pdoyle
 *
 */
@RequiredArgsConstructor(access=PROTECTED)
@EqualsAndHashCode
public class Catalog<E extends Entity> implements Iterable<E>, EnumerableByIdentifier<E> {
	private final ImmutableTreeMap<Identifier, E> elementsById;
	private final ImmutableTreeMap<Long, Identifier> idsBySequence;
	private final ImmutableTreeMap<Identifier, Long> sequenceById;

	public int size() { return elementsById.size(); }

	public boolean isEmpty() { return elementsById.size() == 0; }

	@Override
	public E get(Identifier key) {
		return elementsById.get(requireNonNull(key));
	}

	@Override
	public List<Identifier> ids() {
		return unmodifiableList(new ArrayList<>(idsBySequence.values()));
	}

	public Collection<E> asCollection() {
		return idsBySequence.values().stream()
			.map(elementsById::get)
			.collect(toList());
	}

	public Map<Identifier, E> asMap() {
		Set<Map.Entry<Identifier, E>> entrySet = idsBySequence.values().stream()
			.map(id -> new SimpleImmutableEntry<>(id, elementsById.get(id)))
			.collect(toSet());
		return new AbstractMap<Identifier, E>() {
			@Override
			public Set<Entry<Identifier, E>> entrySet() {
				return entrySet;
			}
		};
	}

	@Override
	public Iterator<E> iterator() {
		Iterator<Identifier> idIterator = idsBySequence.values().iterator();
		return new Iterator<E>() {
			@Override
			public boolean hasNext() {
				return idIterator.hasNext();
			}

			@Override
			public E next() {
				return elementsById.get(idIterator.next());
			}
		};
	}

	public Stream<Identifier> idStream() {
		return idsBySequence.values().stream();
	}

	public Stream<E> stream() {
		return idStream().map(elementsById::get);
	}

	public Spliterator<E> spliterator() {
		// Note that we could add DISTINCT, IMMUTABLE and NONNULL to the
		// characteristics if it turns out to be worth the trouble.  Similar for idStream.
		return stream().spliterator();
	}

	public boolean containsID(Identifier key) {
		return get(key) != null;
	}

	public boolean containsAllIDs(Stream<Identifier> keys) {
		return keys.allMatch(this::containsID);
	}

	public boolean containsAllIDs(Iterable<Identifier> keys) {
		for (Identifier key: keys) {
			if (!containsID(key)) {
				return false;
			}
		}
		return true;
	}

	public boolean contains(E entity) {
		return containsID(entity.id());
	}

	public boolean containsAll(Stream<E> entities) {
		return entities.allMatch(this::contains);
	}

	public boolean containsAll(Iterable<E> entities) {
		for (E entity: entities) {
			if (!contains(entity)) {
				return false;
			}
		}
		return true;
	}

	public static <TT extends Entity> Catalog<TT> empty() {
		return new Catalog<>(
			ImmutableTreeMap.empty(),
			ImmutableTreeMap.empty(),
			ImmutableTreeMap.empty());
	}

	@SafeVarargs
	@SuppressWarnings("varargs")
	public static <TT extends Entity> Catalog<TT> of(TT... entities) {
		return Catalog.of(asList(entities));
	}

	public static <TT extends Entity> Catalog<TT> of(Stream<TT> entities) {
		return Catalog.of(entities.collect(toList()));
	}

	public static <TT extends Entity> Catalog<TT> of(Collection<TT> entities) {
		ImmutableTreeMap<Identifier, TT> elementsById = ImmutableTreeMap.empty();
		ImmutableTreeMap<Long, Identifier> idsBySequence = ImmutableTreeMap.empty();
		ImmutableTreeMap<Identifier, Long> sequenceById = ImmutableTreeMap.empty();
		for (TT entity: entities) {
			if (elementsById.get(entity.id()) != null) {
				throw new IllegalArgumentException("Multiple entities with id " + entity.id());
			}
			elementsById = elementsById.with(entity.id(), entity);
			idsBySequence = idsBySequence.with((long)sequenceById.size(), entity.id());
			sequenceById = sequenceById.with(entity.id(), (long)sequenceById.size());
		}
		return new Catalog<>(elementsById, idsBySequence, sequenceById);
	}

	public Catalog<E> with(E entity) {
		return new Catalog<>(
			this.elementsById.with(entity.id(), entity),
			this.idsBySequence.with((long)sequenceById.size(), entity.id()),
			this.sequenceById.with(entity.id(), (long)sequenceById.size())
		);
	}

	public Catalog<E> withAll(Stream<E> entities) {
		// We need to process the entities multiple times
		List<E> list = entities.collect(toList());
		Map<Identifier, E> newElementsById = new LinkedHashMap<>();
		Map<Long, Identifier> newIdsBySequence = new LinkedHashMap<>();
		Map<Identifier, Long> newSequenceById = new LinkedHashMap<>();
		entities.forEachOrdered(e -> {
//			newElementsById.put(e.id(), e);
//			HEY this needs to be the largest sequence number + 1
//			newIdsBySequence.put(sequenceById.size() + newSequenceById.size());

		});
		Object state = new Object() {
			ImmutableTreeMap<Identifier, E> newElementsById = elementsById;
			ImmutableTreeMap<Long, Identifier> newIdsBySequence = idsBySequence;
			ImmutableTreeMap<Identifier, Long> newSequenceById = sequenceById;
		};
		return new Catalog<>(
			elementsById1,
			idsBySequence1,
			sequenceById1
		);
	}

	public Catalog<E> without(E entity) {
		Map<Identifier, E> newValues = new LinkedHashMap<>(this.contents);
		newValues.remove(requireNonNull(entity.id()));
		return new Catalog<>(unmodifiableMap(newValues));
	}

	public Catalog<E> without(Identifier id) {
		Map<Identifier, E> newValues = new LinkedHashMap<>(this.contents);
		newValues.remove(requireNonNull(id));
		return new Catalog<>(unmodifiableMap(newValues));
	}

	@Override
	public String toString() {
		return contents.toString();
	}

}
