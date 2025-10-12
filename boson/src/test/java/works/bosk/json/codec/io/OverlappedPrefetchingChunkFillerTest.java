package works.bosk.json.codec.io;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class OverlappedPrefetchingChunkFillerTest {

	@Test
	@Order(1) // If this doesn't work, we're pretty hosed
	void eof() {
		byte[] data = new byte[0];
		try (ChunkFiller prefetcher = new OverlappedPrefetchingChunkFiller(new ByteArrayInputStream(data))) {
			assertNull(prefetcher.nextChunk());
		}
	}

	@Test
	void singleSmallBuffer() {
		byte[] data = "abc".getBytes();
		try (ChunkFiller prefetcher = new OverlappedPrefetchingChunkFiller(new ByteArrayInputStream(data), 2, 1)) {
			ByteChunk buf1 = prefetcher.nextChunk();
			assertEquals(2, buf1.length());
			assertArrayEquals(new byte[]{'a', 'b'}, buf1.bytes());
			prefetcher.recycleChunk(buf1);

			ByteChunk buf2 = prefetcher.nextChunk();
			assertEquals(1, buf2.length());
			assertEquals('c', buf2.bytes()[0]);
			prefetcher.recycleChunk(buf2);

			assertNull(prefetcher.nextChunk());
		}
	}

	@Test
	void multipleTinyBuffers() {
		byte[] data = "abcdef".getBytes();
		try (ChunkFiller prefetcher = new OverlappedPrefetchingChunkFiller(new ByteArrayInputStream(data), 1, 3)) {
			List<Byte> read = new ArrayList<>();
			for (int i = 0; i < 6; i++) {
				ByteChunk buf = prefetcher.nextChunk();
				assertEquals(1, buf.length());
				read.add(buf.bytes()[0]);
				prefetcher.recycleChunk(buf);
			}
			assertNull(prefetcher.nextChunk());
			assertEquals(List.of((byte)'a', (byte)'b', (byte)'c', (byte)'d', (byte)'e', (byte)'f'), read);
		}
	}

	@Test
	void recycleAllowsReuse() {
		byte[] data = "xy".getBytes();
		try (ChunkFiller prefetcher = new OverlappedPrefetchingChunkFiller(new ByteArrayInputStream(data), 1, 1)) {
			ByteChunk buf1 = prefetcher.nextChunk();
			assertEquals(1, buf1.length());
			assertArrayEquals(new byte[]{'x'}, buf1.bytes());
			prefetcher.recycleChunk(buf1);

			ByteChunk buf2 = prefetcher.nextChunk();
			assertSame(buf1.bytes(), buf2.bytes());
			assertEquals(1, buf2.length());
			assertArrayEquals(new byte[]{'y'}, buf1.bytes());
			prefetcher.recycleChunk(buf2);

			assertNull(prefetcher.nextChunk());
		}
	}

}
