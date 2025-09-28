package works.bosk.json.mapping.spec;

/**
 * A {@link JsonValueSpec} that represents a JSON <em>string</em>.
 * <p>
 * This is significant because these are the nodes that can be used
 * to specify the representation of member names in a JSON <em>object</em>.
 *
 * @see UniformMapNode
 */
public sealed interface StringSpec extends ScalarSpec permits
	EnumByNameNode,
	StringNode
{ }
