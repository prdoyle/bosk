package io.vena.bosk.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Marks a method to be registered as a hook
 * for an object passed to <code>Bosk.registerHooks</code>.
 * Currently ignored on interface methods.
 */
@Retention(RUNTIME)
@Target(METHOD)
public @interface Hook {
	/**
	 * The scope of the hook for this method.
	 */
	String value();

	/**
	 * Indicates the order in which hooks should run.
	 * Higher numbers run first. Hooks with the same
	 * <code>priority</code> run in bytecode order,
	 * with inherited hooks running first.
	 */
	int priority() default 0;
}
