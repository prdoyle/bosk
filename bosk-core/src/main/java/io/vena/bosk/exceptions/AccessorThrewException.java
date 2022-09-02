package io.vena.bosk.exceptions;

import io.vena.bosk.dereferencers.Dereferencer;
import java.lang.reflect.InvocationTargetException;

/**
 * Wraps an exception that was thrown by a {@link Dereferencer} while
 * applying an update. Indicates a bug in one of the methods called; usually one of the getters
 * or object constructors.
 *
 * <p>
 * Basically a {@link RuntimeException} version of {@link
 * InvocationTargetException}.  {@link #getCause()} returns the original
 * exception; same as {@link InvocationTargetException#getCause()}.
 */
@SuppressWarnings("serial")
public class AccessorThrewException extends RuntimeException {
	public AccessorThrewException(String message, Throwable cause) { super(message, cause); }
}
