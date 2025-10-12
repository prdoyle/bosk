package works.bosk.json.codec.io;

import java.io.IOException;
import java.io.InputStream;

public class SynchronousChunkFiller implements ChunkFiller {
	final InputStream stream;
	final byte[] buffer;

	public SynchronousChunkFiller(InputStream stream) {
		this(stream, 20_000);
	}

	SynchronousChunkFiller(InputStream stream, int bufferSize) {
		this.stream = stream;
		buffer = new byte[bufferSize];
	}

	@Override
	public ByteChunk nextChunk() {
		int length;
		try {
			length = stream.read(buffer);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
		if (length == -1) {
			return null;
		}

		return new ByteChunk(buffer, length);
	}

	@Override
	public void recycleChunk(ByteChunk chunk) {
		assert chunk.bytes() == this.buffer;
	}

	@Override
	public void close() {
		try {
			stream.close();
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}
}
