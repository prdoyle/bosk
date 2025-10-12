package works.bosk.json.codec.io;

/**
 * @param length number of valid bytes in the byte array, starting from index 0.
 */
public record ByteChunk(
	byte[] bytes,
	int length
) { }
