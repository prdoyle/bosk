package works.bosk.json.codec.io;

public class ByteArrayChunkFiller implements ChunkFiller {
	final ByteChunk chunk;

	public ByteArrayChunkFiller(byte[] bytes) {
		this.chunk = new ByteChunk(bytes, bytes.length);
	}

	@Override
	public ByteChunk nextChunk() {
		return chunk;
	}

	@Override
	public void recycleChunk(ByteChunk chunk) {
		assert chunk == this.chunk;
	}

	@Override
	public void close() {
		// Nothing to do
	}
}
