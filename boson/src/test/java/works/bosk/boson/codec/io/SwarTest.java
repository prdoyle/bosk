package works.bosk.boson.codec.io;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.IntPredicate;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the properties of {@link Swar} that the reader relies on:
 * a zero mask means no byte satisfies the test, and the first flagged byte
 * is always a genuine match, even though later flags can be borrow artifacts.
 */
class SwarTest {
	private static final int NUM_RANDOM_WORDS = 1000;

	@Test
	void loadLong_matchesPerByteAssembly() {
		byte[] bytes = new byte[Swar.BYTES * 2];
		new Random(12345).nextBytes(bytes);
		for (int pos = 0; pos < Swar.BYTES; pos++) {
			long expected = 0;
			for (int i = 0; i < Swar.BYTES; i++) {
				expected |= (bytes[pos + i] & 0xFFL) << (8 * i);
			}
			assertEquals(expected, Swar.loadLong(bytes, pos));
		}
	}

	@Test
	void haszero_firstFlaggedByteIsGenuine() {
		for (long word : sampleWords()) {
			assertFirstFlaggedByteIsGenuine(word, b -> b == 0, Swar.haszero(word));
		}
	}

	@Test
	void hasvalue_firstFlaggedByteIsGenuine() {
		int[] values = { 0x00, 0x09, 0x20, 0x22, 0x5C, 0x7F, 0x80, 0xFF };
		for (int value : values) {
			for (long word : sampleWords()) {
				assertFirstFlaggedByteIsGenuine(word, b -> b == value, Swar.hasvalue(word, (byte) value));
			}
		}
	}

	@Test
	void hasless_firstFlaggedByteIsGenuine() {
		int[] limits = { 0x01, 0x09, 0x0E, 0x20, 0x7F, 0x80 };
		for (int limit : limits) {
			for (long word : sampleWords()) {
				assertFirstFlaggedByteIsGenuine(word, b -> b < limit, Swar.hasless(word, limit));
			}
		}
	}

	@Test
	void firstByteOffset_findsLowestSetHighBit() {
		assertEquals(-1, Swar.firstByteOffset(0));
		for (int byteIndex = 0; byteIndex < Swar.BYTES; byteIndex++) {
			long mask = 1L << (8 * byteIndex + 7);
			assertEquals(byteIndex, Swar.firstByteOffset(mask));
			assertEquals(byteIndex, Swar.firstByteOffset(mask | 1L << (8 * (Swar.BYTES - 1) + 7)));
		}
	}

	private static void assertFirstFlaggedByteIsGenuine(long word, IntPredicate byteMatches, long mask) {
		int firstGenuine = firstGenuineMatch(word, byteMatches);
		if (firstGenuine == -1) {
			assertEquals(0, mask, "Word with no matching byte must produce a zero mask");
		} else {
			assertTrue(mask != 0, "Word with a matching byte must produce a nonzero mask");
			assertEquals(firstGenuine, Swar.firstByteOffset(mask),
				"First flagged byte must be a genuine match");
		}
	}

	private static int firstGenuineMatch(long word, IntPredicate byteMatches) {
		for (int i = 0; i < Swar.BYTES; i++) {
			int b = (int) ((word >>> (8 * i)) & 0xFF);
			if (byteMatches.test(b)) {
				return i;
			}
		}
		return -1;
	}

	private static List<Long> sampleWords() {
		List<Long> result = new ArrayList<>();
		result.add(0L);
		result.add(-1L);
		for (int b = 0x00; b <= 0xFF; b += 0x11) {
			long broadcast = 0;
			for (int i = 0; i < Swar.BYTES; i++) {
				broadcast |= (long) b << (8 * i);
			}
			result.add(broadcast);
		}
		for (int byteIndex = 0; byteIndex < Swar.BYTES; byteIndex++) {
			for (int b : new int[] { 0x00, 0x01, 0x09, 0x0A, 0x0D, 0x20, 0x22, 0x5C, 0x7F, 0x80, 0xFF }) {
				long word = 0x4141414141414141L & ~(0xFFL << (8 * byteIndex));
				word |= (long) b << (8 * byteIndex);
				result.add(word);
			}
			// The borrow artifacts show up when a matching byte is followed by
			// a byte one greater (or, for hasless, equal to the limit),
			// so include those patterns explicitly.
			for (int b : new int[] { 0x00, 0x09, 0x20, 0x22, 0x5C, 0x7F }) {
				long word = (long) b;
				word |= (long) (b + 1) << (8 * byteIndex);
				result.add(word);
			}
		}
		Random random = new Random(12345);
		for (int i = 0; i < NUM_RANDOM_WORDS; i++) {
			result.add(random.nextLong());
		}
		return result;
	}
}
