package io.vena.bosk;

import io.vena.bosk.exceptions.InvalidTypeException;

public interface ReferenceFactory<R extends Entity> {
	Reference<R> rootReference();
	<T> Reference<T> reference(Class<T> requestedClass, Path path) throws InvalidTypeException;
	<T extends Entity> CatalogReference<T> catalogReference(Class<T> entryClass, Path path) throws InvalidTypeException;
	<T extends Entity> ListingReference<T> listingReference(Class<T> entryClass, Path path) throws InvalidTypeException;
	<K extends Entity,V> SideTableReference<K,V> sideTableReference(Class<K> keyClass, Class<V> valueClass, Path path) throws InvalidTypeException;
	<T> Reference<Reference<T>> referenceReference(Class<T> targetClass, Path path) throws InvalidTypeException;
}
