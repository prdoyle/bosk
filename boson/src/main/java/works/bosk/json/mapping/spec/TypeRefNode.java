package works.bosk.json.mapping.spec;

import java.lang.reflect.Type;
import works.bosk.json.types.DataType;
import works.bosk.json.types.KnownType;

/**
 * Specifies only that a JSON value should be represented by the given {@code type},
 * where that type's own parsing and generation is specified separately.
 * <p>
 * This enables specification of recursive types, which would otherwise be impossible.
 * It also allows types with shared substructures to be specified without duplication.
 * <p>
 * The means by which the type is parsed and generated is not specified here,
 * and is typically handled by something like {@link works.bosk.json.mapping.TypeScanner TypeScanner}.
 */
public record TypeRefNode (
	KnownType type
) implements JsonValueSpec {
	public static TypeRefNode of(Type type) {
		return new TypeRefNode(DataType.known(type));
	}

	@Override
	public String toString() {
		return "@" + type;
	}

	public KnownType dataType() {
		return type();
	}
}
