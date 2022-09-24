package io.vena.bosk;

import io.vena.bosk.exceptions.NotYetImplementedException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import org.pcollections.HashTreePMap;
import org.pcollections.OrderedPSet;
import org.pcollections.PMap;
import org.pcollections.POrderedSet;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
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
	private final PMap<Identifier, E> contents;
	private final POrderedSet<Identifier> ids; // Preserves insertion order

	// Note: pcollections is in the process of adding an ordered map.
	// We can adopt that once it's ready.
	// https://github.com/hrldcpr/pcollections/issues/95#issuecomment-1247207869

	public int size() { return contents.size(); }

	public boolean isEmpty() { return contents.isEmpty(); }

	@Override
	public E get(Identifier key) {
		return contents.get(requireNonNull(key));
	}

	@Override
	public List<Identifier> ids() {
		return unmodifiableList(new ArrayList<>(ids));
	}

	public Collection<E> asCollection() {
		return unmodifiableList(ids.stream().map(contents::get).collect(toList()));
	}

	public Map<Identifier, E> asMap() {
		return unmodifiableMap(ids.stream().collect(toMap(
			id -> id,
			contents::get,
			(a,b) ->{ throw new NotYetImplementedException(); },
			LinkedHashMap::new)));
	}

	@Override
	public Iterator<E> iterator() {
		return stream().iterator();
	}

	public Stream<Identifier> idStream() {
		return ids.stream();
	}

	public Stream<E> stream() {
		return ids.stream().map(contents::get);
	}

	public Spliterator<E> spliterator() {
		// Note that we could add DISTINCT, IMMUTABLE and NONNULL to the
		// characteristics if it turns out to be worth the trouble.  Similar for idStream.
		return stream().spliterator();
	}

	public boolean containsID(Identifier key) {
		return ids.contains(key);
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
		return new Catalog<>(HashTreePMap.empty(), OrderedPSet.empty());
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
		Map<Identifier, TT> newValues = new LinkedHashMap<>(entities.size());
		for (TT entity: entities) {
			TT old = newValues.put(requireNonNull(entity.id()), entity);
			if (old != null) {
				throw new IllegalArgumentException("Multiple entities with id " + old.id());
			}
		}
		return new Catalog<>(
			HashTreePMap.from(newValues),
			OrderedPSet.from(entities.stream().map(Entity::id).collect(toList())));
	}

	public Catalog<E> with(E entity) {
		return new Catalog<>(contents.plus(entity.id(), entity), ids.plus(entity.id()));
	}

	public Catalog<E> withAll(Stream<E> entities) {
		PMap<Identifier, E> newContents = contents;
		POrderedSet<Identifier> newIDs = ids;
		Iterator<E> iter = entities.iterator();
		while (iter.hasNext()) {
			E entity = iter.next();
			newContents = newContents.plus(entity.id(), entity);
			newIDs = newIDs.plus(entity.id());
		}
		return new Catalog<>(newContents, newIDs);
	}

	public Catalog<E> without(E entity) {
		return new Catalog<>(contents.minus(entity.id()), ids.minus(entity.id()));
	}

	public Catalog<E> without(Identifier id) {
		return new Catalog<>(contents.minus(id), ids.minus(id));
	}

	@Override
	public String toString() {
		return contents.toString();
	}

}
