package works.bosk.boson.mapping;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.RECORD_COMPONENT;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Indicates that a record component can be null, and is to be represented in the JSON as {@code null}.
 * This is subtly different from other nullability annotations, which say nothing about JSON.
 * For example, one could imagine a nullable field which is to be represented by some default value in the JSON instead.
 * <p>
 * TODO: I'm not sure this is a useful distinction. Perhaps we should delete this annotation and use the JSpecify one.
 */
@Retention(RUNTIME)
@Target(RECORD_COMPONENT)
public @interface Nullable {
}
