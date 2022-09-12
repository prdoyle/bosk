package io.vena.bosk;

import java.util.Comparator;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@EqualsAndHashCode
public final class Identifier implements Comparable<Identifier> {
	@NonNull final String value;

	/**
	 * Compares by lexicographic order on the string representation.
	 */
	@Override
	public int compareTo(Identifier other) {
		return value.compareTo(other.value);
	}

	// TODO: Intern these.  No need to have several Identifier objects for the same value
	public static Identifier from(String value) {
		if (value.isEmpty()) {
			throw new IllegalArgumentException("Identifier can't be empty");
		} else if (value.startsWith("-") || value.endsWith("-")) {
			throw new IllegalArgumentException("Identifier can't start or end with a hyphen");
		}
		// TODO: We probably ought to outlaw some characters like NUL (\u0000) but
		//  that's O(n) in the length of the string, so it's not clear that's worth the overhead.
		return new Identifier(value);
	}

	/**
	 * I'm going to regret adding this.
	 */
	public static synchronized Identifier unique(String prefix) {
		return new Identifier(prefix + (++uniqueIdCounter));
	}

	private static long uniqueIdCounter = 1000;

	@Override public String toString() { return value; }

	public static final Comparator<Identifier> LEXICAL_ORDER = Identifier::compareTo;
}
