package works.bosk;

/**
 * An entity that can return a {@link #reference()} to itself.
 *
 * <p>
 * Because the bosk system identifies an object by its location in the document
 * tree, this means instances of this class have enough information to determine
 * their identity, and so we provide some recommended {@link #equals(Object) equals}
 * and {@link #hashCode() hashCode} implementations.
 *
 * <p>
 * <em>Performance note</em>: References aren't cheap to create.
 *
 * @author Patrick Doyle
 */
public interface ReflectiveEntity<T extends ReflectiveEntity<T>> extends Entity {
	Reference<T> reference();

	default boolean reflectiveEntity_equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (obj instanceof ReflectiveEntity<?> r) {
			return this.reference().equals(r.reference());
		} else {
			return false;
		}
	}

	default String reflectiveEntity_toString() {
		return getClass().getSimpleName() + "(" + reference() + ")";
	}
}
