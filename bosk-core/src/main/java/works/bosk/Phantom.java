package works.bosk;

/**
 * Used to create a thing that can be referenced (eg. as the domain
 * of a {@link Listing}) but that is not actually serialized or instantiated.
 * Behaves like an {@link java.util.Optional Optional} that is always empty.
 * <p>
 * Why would anyone want such a thing?
 * <p>
 * The domain of a {@link Listing} plays a role somewhat like a datatype:
 * it indicates the nature of the values it can reference.
 * Often, the domain is a {@link Catalog} that explicitly lists all the allowed values,
 * but sometimes you don't want to list all the possible values in the bosk state;
 * you just want to be clear about what the allowed values are.
 * For example, there may be a large or infinite number of possible values;
 * or there might be nothing to store about the value besides its ID.
 * In that case, you can use a <code>Phantom&lt;Catalog&lt;T>></code>,
 * and reference that as your domain.
 */
public final class Phantom<T> {
	private Phantom() {}

	public static <T> Phantom<T> empty() {
		@SuppressWarnings("unchecked")
		Phantom<T> instance = (Phantom<T>) INSTANCE;
		return instance;
	}

	@Override
	public String toString() {
		return "Phantom.empty";
	}

	private static final Phantom<?> INSTANCE = new Phantom<>();
}
