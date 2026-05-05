package works.bosk.junit;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Identifies a crucial baseline test that must pass before any other tests in the class are even attempted.
 * Requires {@link RunAnteTestsFirst} on the class to activate the behaviour;
 * otherwise, this annotation silently does nothing.
 * <p>
 * Methods annotated with this will run before other tests.
 * If any {@code @Ante} test method fails, all remaining tests in the class are skipped.
 * For class templates, only the current invocation of the template is skipped.
 * <p>
 * Use in conjunction with {@code @Test} or other similar annotations as appropriate.
 */
@Target(METHOD)
@Retention(RUNTIME)
public @interface Ante {}
