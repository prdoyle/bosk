package works.bosk;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import works.bosk.exceptions.InvalidTypeException;
import works.bosk.exceptions.NonexistentReferenceException;

import static java.util.Arrays.asList;

/**
 * A pointer to an object stored at a known location in the document tree.
 * The location is indicated by {@link #path()}.
 *
 * @author pdoyle
 *
 * @param <T> The type of object being referenced.
 */
public sealed interface Reference<T> permits
	CatalogReference,
	ListingReference,
	RootReference,
	SideTableReference,
	Bosk.ReferenceImpl
{
	Path path();

	/**
	 * @return For generic {@link #value()} types, returns a {@link ParameterizedType}; otherwise, same as {@link #targetClass()}.
	 */
	Type targetType();

	/**
	 * @return The class that {@link #value()} is an <code>instanceof</code>.
	 */
	Class<T> targetClass();

	/**
	 * @return The value of the referenced object, or <code>null</code> if {@link #path()} refers to a nonexistent object
	 * @throws IllegalStateException if there is no open {@link Bosk.ReadContext} on this thread
	 */
	T valueIfExists();

	/**
	 * @return The value of the referenced object
	 * @throws NonexistentReferenceException if {@link #path()} refers to a nonexistent object
	 * @throws IllegalStateException if there is no open {@link Bosk.ReadContext} on this thread
	 */
	default T value() {
		T result = valueIfExists();
		if (result == null) {
			throw new NonexistentReferenceException(this);
		} else {
			return result;
		}
	}

	/**
	 * @return false iff {@link #path()} refers to a nonexistent object
	 * @throws IllegalStateException if there is no open {@link Bosk.ReadContext} on this thread
	 * @see #valueIfExists()
	 */
	default boolean exists() {
		return valueIfExists() != null;
	}

	/**
	 * @return <code>Optional.ofNullable(valueIfExists())</code>.
	 */
	default Optional<T> optionalValue() {
		return Optional.ofNullable(valueIfExists());
	}

	default T valueOrDefault(T defaultValue) {
		T result = valueIfExists();
		if (result == null) {
			return defaultValue;
		} else {
			return result;
		}
	}

	default T valueOrElse(Supplier<T> defaultSupplier) {
		T result = valueIfExists();
		if (result == null) {
			return defaultSupplier.get();
		} else {
			return result;
		}
	}

	void forEachValue(BiConsumer<T, BindingEnvironment> action, BindingEnvironment existingEnvironment);

	default void forEachValue(BiConsumer<T, BindingEnvironment> action) {
		forEachValue(action, BindingEnvironment.empty());
	}

	default void forEachValue(Consumer<T> action) {
		forEachValue((v,e)->action.accept(v));
	}

	default String pathString() {
		return path().urlEncoded();
	}

	default Identifier idAt(int segmentNum) { return Identifier.from(path().segment(segmentNum)); }

	/**
	 * Any parameters bound in <code>bindings</code> are replaced by their values.
	 * Any parameters not bound in <code>bindings</code> are left as parameters.
	 * Any additional <code>bindings</code> are ignored.
	 * @return a reference like this one, but with any parameters bound in <code>bindings</code>
	 * replaced by their values
	 */
	Reference<T> boundBy(BindingEnvironment bindings);

	/**
	 * @return a reference like this one, but with the first N parameters bound to
	 * the given values, where N is the number of <code>ids</code>
	 * @throws IllegalArgumentException if this reference has fewer parameters than
	 * the number of <code>ids</code>
	 */
	default Reference<T> boundTo(Identifier... ids) {
		return this.boundBy(path().parametersFrom(asList(ids)));
	}

	/**
	 * @param definitePath A Path with {@link Path#numParameters()} of zero whose segments
	 * all match those of this reference's path, up to the limit of whichever one is shorter
	 * @return A Reference like this one, but with as many as possible of its parameters substituted with
	 * values taken from <code>definitePath</code>.
	 */
	default Reference<T> boundBy(Path definitePath) {
		return this.boundBy(parametersFrom(definitePath));
	}

	/**
	 * @return <code>path().{@link Path#parametersFrom parametersFrom}(definitePath)</code>
	 */
	default BindingEnvironment parametersFrom(Path definitePath) {
		return path().parametersFrom(definitePath);
	}

	default boolean isRoot() {
		return path().isEmpty();
	}

	/**
	 * @return The equivalent of {@link Bosk#rootReference()} on the <code>bosk</code> to which this Reference applies,
	 * but without static type checking; the intent is that you'd call {@link #then} on the resulting reference,
	 * and that's when type checking occurs.
	 */
	RootReference<?> root();

	/**
	 * @param <U> The type of <code>targetClass</code>
	 * @param targetClass The class of the object being referenced
	 * @param segments The {@link Path#segment(int) segments} to add to {@link #path()}
	 * @return The equivalent of <code>bosk.reference(targetClass, {@link #path()}.{@link Path#then(String...) then}(segments))</code> on the <code>bosk</code> to which this Reference applies.
	 * @throws InvalidTypeException if the referenced object is not assignable to <code>targetClass</code>.
	 */
	<U> Reference<U> then(Class<U> targetClass, String...segments) throws InvalidTypeException;

	<E extends Entity> CatalogReference<E> thenCatalog(Class<E> entryClass, String... segments) throws InvalidTypeException;
	<E extends Entity> ListingReference<E> thenListing(Class<E> entryClass, String... segments) throws InvalidTypeException;
	<K extends Entity,V> SideTableReference<K,V> thenSideTable(Class<K> keyClass, Class<V> valueClass, String... segments) throws InvalidTypeException;
	<TT> Reference<Reference<TT>> thenReference(Class<TT> targetClass, String... segments) throws InvalidTypeException;

	<TT> Reference<TT> truncatedTo(Class<TT> targetClass, int remainingSegments) throws InvalidTypeException;

	/**
	 * Returns a {@link Reference} with the {@link Path#lastSegment() last segment} removed.
	 *
	 * <p>
	 * <em>Design note</em>: we'd usually throw {@link InvalidTypeException} if the caller passes something invalid in here.
	 * However, experience has shown that almost every single time this is called, the user knows the call will always succeed,
	 * and just catches the {@link InvalidTypeException} and wraps it in an {@link AssertionError}.
	 * @param targetClass Type constraint on the reference; the returned
	 * reference will satisfy <code>targetClass.{@link Class#isAssignableFrom isAssignableFrom}(result.{@link
	 * #targetClass()})</code>.
	 * @return a {@link Reference} whose {@link #path()} is a proper prefix of
	 * this.{@link #path()}, and whose {@link #targetClass()} conforms to
	 * <code>targetClass</code>.
	 * @throws IllegalArgumentException if the enclosing reference does not point to the specified {@code targetClass},
	 * or if this reference is the root reference and therefore has no last segment to remove.
	 */
	<TT> Reference<TT> enclosingReference(Class<TT> targetClass);

	/**
	 * @return <code>this.path().{@link Path#isPrefixOf isPrefixOf}(other.path())</code>
	 * @see Path#isPrefixOf
	 */
	default boolean encloses(Reference<?> other) {
		return this.path().isPrefixOf(other.path());
	}

	/**
	 * Two references are equal if they have the same root type and the same path
	 * (even if they come from two different Bosks).
	 */
	@Override boolean equals(Object obj);
	@Override int hashCode();
}
