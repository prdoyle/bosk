package works.bosk.json.codec.io;

import java.nio.ByteBuffer;

public interface BufferFiller extends AutoCloseable {
	ByteBuffer nextBuffer();

	void recycleBuffer(ByteBuffer buffer);

	@Override void close(); // No throws Exception
}
