package works.bosk.json.codec.io;

public interface ChunkFiller extends AutoCloseable {
	ByteChunk nextChunk();

	void recycleChunk(ByteChunk chunk);

	@Override void close(); // No throws Exception
}
