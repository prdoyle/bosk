package works.bosk.json.codec.io;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class OverlappedPrefetcherTest {

	@Test
	@Order(1) // If this doesn't work, we're pretty hosed
	void eof() {
		byte[] data = new byte[0];
		try (OverlappedPrefetcher prefetcher = new OverlappedPrefetcher(channelFromBytes(data))) {
			assertNull(prefetcher.nextBuffer());
		}
	}

	@Test
	void singleSmallBuffer() {
		byte[] data = "abc".getBytes();
		try (OverlappedPrefetcher prefetcher = new OverlappedPrefetcher(channelFromBytes(data), 2, 1)) {
			ByteBuffer buf1 = prefetcher.nextBuffer();
			assertEquals(2, buf1.remaining());
			assertEquals((byte) 'a', buf1.get());
			assertEquals((byte) 'b', buf1.get());
			prefetcher.recycleBuffer(buf1);

			ByteBuffer buf2 = prefetcher.nextBuffer();
			assertEquals(1, buf2.remaining());
			assertEquals((byte) 'c', buf2.get());
			prefetcher.recycleBuffer(buf2);

			assertNull(prefetcher.nextBuffer());
		}
	}

	@Test
	void multipleTinyBuffers() {
		byte[] data = "abcdef".getBytes();
		try (OverlappedPrefetcher prefetcher = new OverlappedPrefetcher(channelFromBytes(data), 1, 3)) {
			List<Byte> read = new ArrayList<>();
			for (int i = 0; i < 6; i++) {
				ByteBuffer buf = prefetcher.nextBuffer();
				assertEquals(1, buf.remaining());
				read.add(buf.get());
				prefetcher.recycleBuffer(buf);
			}
			assertNull(prefetcher.nextBuffer());
			assertEquals(List.of((byte)'a', (byte)'b', (byte)'c', (byte)'d', (byte)'e', (byte)'f'), read);
		}
	}

	@Test
	void recycleAllowsReuse() {
		byte[] data = "xy".getBytes();
		try (OverlappedPrefetcher prefetcher = new OverlappedPrefetcher(channelFromBytes(data), 1, 1)) {
			ByteBuffer buf1 = prefetcher.nextBuffer();
			assertEquals(1, buf1.remaining());
			assertEquals((byte) 'x', buf1.get());
			prefetcher.recycleBuffer(buf1);

			ByteBuffer buf2 = prefetcher.nextBuffer();
			assertSame(buf1, buf2);
			assertEquals(1, buf2.remaining());
			assertEquals((byte) 'y', buf2.get());
			prefetcher.recycleBuffer(buf2);

			assertNull(prefetcher.nextBuffer());
		}
	}

	private static ReadableByteChannel channelFromBytes(byte[] data) {
		return Channels.newChannel(new ByteArrayInputStream(data));
	}

}
