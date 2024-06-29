package works.bosk.util;

import works.bosk.Catalog;
import works.bosk.CatalogReference;
import works.bosk.Entity;
import works.bosk.EnumerableByIdentifier;
import works.bosk.ListValue;
import works.bosk.Listing;
import works.bosk.ListingReference;
import works.bosk.MapValue;
import works.bosk.Reference;
import works.bosk.SideTable;
import works.bosk.SideTableReference;

/**
 * An imperfect, non-idiomatic way to describe complex parameterized types.
 *
 * <p>
 * To represent the type <code>SideTable&lt;X, Listing&lt;Y>></code>, for example,
 * write <code>sideTable(X.class, listing(Y.class))</code>.
 */
public abstract class Classes {
	@SuppressWarnings({"unchecked","rawtypes","unused"})
	public static <E extends Entity> Class<Catalog<E>> catalog(Class<E> entryClass) {
		return (Class)Catalog.class;
	}

	@SuppressWarnings({"unchecked","rawtypes","unused"})
	public static <E extends Entity> Class<Listing<E>> listing(Class<E> entryClass) {
		return (Class)Listing.class;
	}

	@SuppressWarnings({"unchecked","rawtypes","unused"})
	public static <K extends Entity,V> Class<SideTable<K,V>> sideTable(Class<K> keyClass, Class<V> valueClass) {
		return (Class) SideTable.class;
	}

	@SuppressWarnings({"unchecked","rawtypes","unused"})
	public static <E extends Entity> Class<EnumerableByIdentifier<E>> enumerableByIdentifier(Class<E> entryClass) {
		return (Class) EnumerableByIdentifier.class;
	}

	@SuppressWarnings({"unchecked","rawtypes","unused"})
	public static <T> Class<Reference<T>> reference(Class<T> targetClass) {
		return (Class)Reference.class;
	}

	@SuppressWarnings({"unchecked","rawtypes","unused"})
	public static <E extends Entity> Class<CatalogReference<E>> catalogReference(Class<E> targetClass) {
		return (Class)CatalogReference.class;
	}

	@SuppressWarnings({"unchecked","rawtypes","unused"})
	public static <E extends Entity> Class<ListingReference<E>> listingReference(Class<E> targetClass) {
		return (Class)ListingReference.class;
	}

	@SuppressWarnings({"unchecked","rawtypes","unused"})
	public static <K extends Entity,V> Class<SideTableReference<K,V>> sideTableReference(Class<K> keyClass, Class<V> valueClass) {
		return (Class) SideTableReference.class;
	}

	@SuppressWarnings({"unchecked","rawtypes","unused"})
	public static <E> Class<ListValue<E>> listValue(Class<E> entryClass) {
		return (Class)ListValue.class;
	}

	@SuppressWarnings({"unchecked","rawtypes","unused"})
	public static <V> Class<MapValue<V>> mapValue(Class<V> valueClass) {
		return (Class)MapValue.class;
	}

}
