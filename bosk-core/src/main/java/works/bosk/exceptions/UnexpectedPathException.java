package works.bosk.exceptions;

import works.bosk.Path;

/**
 * Indicates a {@link Path} was encountered that is well-formed, but invalid in context.
 */
public class UnexpectedPathException extends IllegalArgumentException {
	public UnexpectedPathException(String message) { super(message); }
	public UnexpectedPathException(Throwable cause) { super(cause); }
	public UnexpectedPathException(String message, Throwable cause) { super(message, cause); }
}
