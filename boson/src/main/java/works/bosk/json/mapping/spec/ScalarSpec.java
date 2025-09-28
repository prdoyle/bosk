package works.bosk.json.mapping.spec;

/**
 * A node corresponding to a JSON <em>value</em> that is not an <em>array</em> or <em>object</em>.
 *
 * @see ArraySpec
 * @see ObjectSpec
 */
public sealed interface ScalarSpec extends JsonValueSpec permits
	BigNumberNode,
	BooleanNode,
	BoxedPrimitiveSpec,
	PrimitiveNumberNode,
	StringSpec
{
}
