package works.bosk.graphql.exceptions;

/**
 * Thrown when a type name or field name from the Bosk state tree
 * cannot be represented in the generated GraphQL schema.
 * <p>
 * This can happen when:
 * <ul>
 *   <li>A Java class name contains a {@code $} character, which is not valid
 *       in a GraphQL type name
 *   <li>Two classes in the state tree have the same {@link Class#getSimpleName() simple name},
 *       which would produce a GraphQL type name collision
 *   <li>A class name starts with {@code _}, which is reserved for generated types
 *       in the GraphQL schema
 *   <li>A field name contains a {@code $} character
 *   <li>A field name starts with {@code __} (double underscore), which is reserved
 *       for GraphQL introspection
 * </ul>
 * In such cases, the offending type will need to be renamed or wrapped
 * to provide a GraphQL-suitable name.
 */
@SuppressWarnings("serial")
public class UnsupportedNameException extends RuntimeException {
	public UnsupportedNameException(String message) {
		super(message);
	}
	public UnsupportedNameException(String message, Throwable cause) {
		super(message, cause);
	}
}
