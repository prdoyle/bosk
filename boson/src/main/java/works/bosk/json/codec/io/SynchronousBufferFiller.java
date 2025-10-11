package works.bosk.json.codec.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class SynchronousBufferFiller implements BufferFiller {
	final ReadableByteChannel channel;
	final ByteBuffer buffer;

	public SynchronousBufferFiller(ReadableByteChannel channel) {
		this(channel, 16*1024);
	}

	SynchronousBufferFiller(ReadableByteChannel channel, int bufferSize) {
		this.channel = channel;
		buffer = ByteBuffer.allocate(bufferSize);
	}

	@Override
	public ByteBuffer nextBuffer() {
		int read;
		try {
			read = channel.read(buffer);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
		if (read == -1) {
			return null;
		}

		buffer.flip();
		return buffer;
	}

	@Override
	public void recycleBuffer(ByteBuffer buffer) {
		assert this.buffer == buffer;
		buffer.clear();
	}

	@Override
	public void close() {
		try {
			channel.close();
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}
}
