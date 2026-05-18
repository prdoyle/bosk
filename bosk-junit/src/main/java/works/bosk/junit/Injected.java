package works.bosk.junit;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Marks a field or parameter on a test class/method for injection via {@link InjectFrom}.
 * <p>
 * The field/parameter will be set by the injection machinery before each test method
 * invocation, using values provided by the {@link Injector}s declared in {@code @InjectFrom}.
 *
 * @see InjectFrom
 */
@Retention(RUNTIME)
@Target({FIELD, PARAMETER})
public @interface Injected {
	/**
	 * Optional qualifier for this injection site. Empty string means no qualifier.
	 * For parameters that use the same {@link Injector},
	 * if they have the same qualifier, they receive the same value;
	 * if they have different qualifiers, they receive the Cartesian product of value combinations.
	 */
	String value() default "";
}
