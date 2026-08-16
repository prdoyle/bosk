package works.bosk.boson.codec.io;

/**
 * SIMD-within-a-register (SWAR) helpers: bit-parallel tests over the bytes of a {@code long}.
 * <p>
 * Each helper treats the {@code long} as 8 independent bytes and returns a mask with
 * bit 7 of each byte set iff that byte satisfies the test.
 * <p>
 * The classic subtract-based formulas these helpers use are affected by borrow
 * propagation: a byte immediately following a satisfying byte can be falsely flagged.
 * The false flag is always at a higher byte offset than the genuine match that caused it,
 * so {@link #firstByteOffset} always identifies a genuine match, and a zero mask means
 * no byte satisfies the test. Callers must not rely on the exactness of any flag other
 * than the first.
 * <p>
 * {@link #loadLong} assembles bytes with the first byte in the least significant position,
 * so the bit manipulation behaves the same regardless of the endianness of the CPU.
 * The HotSpot JIT recognizes the assembly pattern and lowers it to a single word load
 * on little-endian machines.
 */
final class Swar {
	/**
	 * The number of bytes processed at a time.
	 */
	static final int BYTES = Long.BYTES;

	private static final long LOW_BITS = 0x0101010101010101L;
	private static final long HIGH_BITS = 0x8080808080808080L;

	/**
	 * Assembles the 8 bytes starting at {@code pos} into a word,
	 * with the first byte in the least significant position.
	 */
	static long loadLong(byte[] bytes, int pos) {
		return (bytes[pos] & 0xFFL)
			| ((bytes[pos + 1] & 0xFFL) << 8)
			| ((bytes[pos + 2] & 0xFFL) << 16)
			| ((bytes[pos + 3] & 0xFFL) << 24)
			| ((bytes[pos + 4] & 0xFFL) << 32)
			| ((bytes[pos + 5] & 0xFFL) << 40)
			| ((bytes[pos + 6] & 0xFFL) << 48)
			| ((bytes[pos + 7] & 0xFFL) << 56);
	}

	/**
	 * Bit 7 of each byte set iff that byte is zero.
	 */
	static long haszero(long word) {
		return (word - LOW_BITS) & ~word & HIGH_BITS;
	}

	/**
	 * Bit 7 of each byte set iff that byte equals {@code value}.
	 */
	static long hasvalue(long word, byte value) {
		return haszero(word ^ ((value & 0xFFL) * LOW_BITS));
	}

	/**
	 * Bit 7 of each byte set iff that byte is strictly less than {@code limit},
	 * comparing bytes as unsigned values.
	 * <p>
	 * The bit trick is only valid for limits of at most 128, which covers all
	 * characters of interest in JSON.
	 */
	static long hasless(long word, int limit) {
		assert 0 <= limit && limit <= 128;
		return (word - ((long) limit) * LOW_BITS) & ~word & HIGH_BITS;
	}

	/**
	 * Bit 7 of each byte set iff that byte is non-ASCII, i.e. its high bit is set.
	 * Unlike the other helpers, this one is exact because it involves no arithmetic.
	 */
	static long hasHighBit(long word) {
		return word & HIGH_BITS;
	}

	/**
	 * The offset of the first byte with bit 7 set, or -1 if no byte has bit 7 set.
	 */
	static int firstByteOffset(long mask) {
		if (mask == 0) {
			return -1;
		} else {
			return Long.numberOfTrailingZeros(mask) >> 3;
		}
	}
}
