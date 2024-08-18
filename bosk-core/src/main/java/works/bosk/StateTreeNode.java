package works.bosk;

/**
 * An object that can represent one vertex of the tree of objects maintained
 * by a {@link Bosk}.
 * You can also use a {@code record}; this interface is only if, for some reason,
 * you need to use a class that is not a record.
 *
 * <p>
 * Essentially a marker interface indicating that an object is a willing
 * participant in the Boskiverse.  Various default behaviour (like
 * deserialization) will work differently from other less cooperative Java objects.
 *
 * @author pdoyle
 */
public interface StateTreeNode {

}
