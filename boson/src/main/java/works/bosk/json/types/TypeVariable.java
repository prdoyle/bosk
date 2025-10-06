package works.bosk.json.types;

/**
 * Note that we don't handle type bounds here. This is very subtle.
 * <p>
 * For type variables, bounds appear only at the declaration site, not at the use site.
 * For our purposes, we're dealing only with type expressions, not type declarations,
 * so we never encounter a situation where variable type bounds matter anyway.
 * <p>
 * This is different for {@link WildcardType}s, where bounds appear at the use site,
 * in the type expression itself. That is, you can declare a variable of type
 * {@code List<? extends Number>}, but you cannot declare one of type
 * {@code List<T extends Number>}.
 */
public record TypeVariable(String name) implements UnknownType {
	@Override
	public String toString() {
		return name;
	}

	@Override
	public boolean isAssignableFrom(DataType other) {
		return true;
	}
}
