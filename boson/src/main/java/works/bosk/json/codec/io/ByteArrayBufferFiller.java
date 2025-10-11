package works.bosk.json.codec.io;

import java.nio.ByteBuffer;

class ByteArrayBufferFiller implements BufferFiller {
	final ByteBuffer buffer;

	public ByteArrayBufferFiller(byte[] bytes) {
		this.buffer = ByteBuffer.wrap(bytes);
	}

	@Override
	public ByteBuffer nextBuffer() {
		return buffer;
	}

	@Override
	public void recycleBuffer(ByteBuffer buffer) {
		assert buffer == this.buffer;
	}

	@Override
	public void close() {
		// Nothing to do
	}
}
