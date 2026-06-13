package works.bosk.graphql.exceptions;

/**
 * Thrown when the generator encounters a type from the Bosk state tree
 * that cannot be mapped to a GraphQL type.
 * <p>
 * This can happen when:
 * <ul>
 *   <li>A field type is not a known Bosk collection, record, enum, or scalar type
 *   <li>A parameterized wrapper type (e.g. {@code Optional<Catalog<...>>}) is not
 *       yet supported by the schema generator
 *   <li>A type has an unsupported generic structure
 * </ul>
 * In such cases, the offending type will need to be replaced or reshaped
 * to make it representable in the generated GraphQL schema.
 */
@SuppressWarnings("serial")
public class UnsupportedTypeException extends RuntimeException {
	public UnsupportedTypeException(String message) {
		super(message);
	}
	public UnsupportedTypeException(String message, Throwable cause) {
		super(message, cause);
	}
}
