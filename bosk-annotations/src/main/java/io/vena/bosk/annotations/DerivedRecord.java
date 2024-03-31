package io.vena.bosk.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Marks a type as being a short-lived value, computed from bosk structures,
 * whose <code>Entity</code> fields do not denote a containment operation as they
 * usually do in a bosk structure.
 * This allows you to create data structures referencing bosk objects without
 * the hassle of pedantically using explicit <code>Reference</code>s everywhere.
 *
 * <p>
 * "Short-lived" here refers to the fact that a <code>DerivedRecord</code> is derived
 * from a snapshot of the state of the bosk because it contains objects rather
 * than <code>Reference</code>s. It will not evolve along with the bosk, so it's
 * suitable, for example, during a single HTTP request; but if retained longer,
 * it runs the risk of holding onto old state that may be incorrect, and may
 * increase the application's memory footprint by retaining multiple
 * "generations" of the same objects.
 *
 * <p>
 * During JSON serialization, any <code>ReflectiveEntity</code> fields are serialized
 * and deserialized as though they were <code>Reference</code>s, which is probably
 * what you want. This means that deserialization must be done inside a
 * <code>ReadContext</code>.
 *
 * <p>
 * A <code>DerivedRecord</code> object is not valid in a bosk. If serialized or
 * deserialized, it is not permitted to have any <code>Entity</code> fields unless
 * they are also instances of <code>ReflectiveEntity</code>.
 *
 * @author Patrick Doyle
 *
 */
@Retention(RUNTIME)
@Target(TYPE)
public @interface DerivedRecord {

}
