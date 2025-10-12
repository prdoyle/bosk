package works.bosk.json.codec.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Uses a virtual thread to read from a channel in the background
 * while the foreground thread processes previously read data.
 * This ensures that the parsing runs at the full speed of either
 * the processing or the I/O, whichever is slower.
 * Without the background thread, the processing and I/O would
 * be interleaved, so neither would run at full speed.
 * <p>
 * Calling {@link #close()} will close the underlying channel.
 */
public final class OverlappedPrefetcher implements BufferFiller {
	private final ReadableByteChannel channel;
	private final BlockingQueue<ByteBuffer> emptyBuffers;
	private final BlockingQueue<ByteBuffer> filledBuffers;
	private final Thread backgroundThread;

	public OverlappedPrefetcher(ReadableByteChannel channel) {
		this(channel, 16*1024, 2);
	}

	/**
	 * @param numBuffers if only 1, no overlapping will occur.
	 */
	public OverlappedPrefetcher(ReadableByteChannel channel, int bufferSize, int numBuffers) {
		this.channel = channel;
		this.emptyBuffers = new ArrayBlockingQueue<>(numBuffers);
		this.filledBuffers = new ArrayBlockingQueue<>(numBuffers);

		for (int i = 0; i < numBuffers; i++) {
			emptyBuffers.add(ByteBuffer.allocate(bufferSize));
		}

		backgroundThread = Thread.ofVirtual()
			.name("overlapped-prefetcher")
			.start(this::fillBuffers);
	}

	private void fillBuffers() {
		try {
			while (true) {
				ByteBuffer buffer = emptyBuffers.take();
				int read = channel.read(buffer);
				if (read == -1) {
					filledBuffers.put(EOF_SENTINEL);
					break;
				}

				buffer.flip();
				filledBuffers.put(buffer);
			}
		} catch (ClosedChannelException _) {
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * @return the next filled buffer, or null if EOF.
	 */
	@Override
	public ByteBuffer nextBuffer() {
		ByteBuffer result;
		try {
			result = filledBuffers.take();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return null;
		}
		if (result == EOF_SENTINEL) {
			return null;
		} else {
			return result;
		}
	}

	/**
	 * Recycle a buffer after use.
	 */
	@Override
	public void recycleBuffer(ByteBuffer buffer) {
		buffer.clear();
		emptyBuffers.offer(buffer);
	}

	@Override
	public void close() {
		backgroundThread.interrupt();
		try {
			channel.close();
		} catch (IOException _) {}
	}

	private static final ByteBuffer EOF_SENTINEL = ByteBuffer.allocate(0);
}
