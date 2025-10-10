package works.bosk.json.codec.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Uses a virtual thread to read from a channel in the background
 * while the foreground thread processes previously read data.
 */
final class OverlappedPrefetcher implements AutoCloseable {
	private final ReadableByteChannel channel;
	private final ArrayBlockingQueue<ByteBuffer> emptyBuffers;
	private final ArrayBlockingQueue<ByteBuffer> filledBuffers;
	private final Thread backgroundThread;
	private final AtomicBoolean running = new AtomicBoolean(true);

	OverlappedPrefetcher(ReadableByteChannel channel) {
		this(channel, 16*1024, 2);
	}

	/**
	 * @param numBuffers if only 1, no overlapping will occur.
	 */
	OverlappedPrefetcher(ReadableByteChannel channel, int bufferSize, int numBuffers) {
		this.channel = channel;
		this.emptyBuffers = new ArrayBlockingQueue<>(numBuffers);
		this.filledBuffers = new ArrayBlockingQueue<>(numBuffers);

		for (int i = 0; i < numBuffers; i++) {
			emptyBuffers.add(ByteBuffer.allocateDirect(bufferSize));
		}

		backgroundThread = Thread.ofVirtual()
			.name("overlapped-prefetcher")
			.start(this::fillBuffers);
	}

	private void fillBuffers() {
		try {
			while (running.get()) {
				ByteBuffer buffer = emptyBuffers.take();
				int read = channel.read(buffer);
				if (read == -1) {
					filledBuffers.put(EOF_SENTINEL);
					running.set(false);
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
	public void recycleBuffer(ByteBuffer buffer) {
		buffer.clear();
		emptyBuffers.offer(buffer);
	}

	@Override
	public void close() {
		running.set(false);
		try {
			channel.close();
		} catch (IOException ignored) {}
		backgroundThread.interrupt();
	}

	private static final ByteBuffer EOF_SENTINEL = ByteBuffer.allocate(0);
}
