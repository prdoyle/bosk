package io.vena.bosk.codecs;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonWriter;
import io.vena.bosk.codecs.GsonAdapterCompiler.Codec;
import java.io.IOException;

public abstract class CodecRuntime implements Codec {
	/**
	 * Looks up a {@link TypeAdapter} at serialization time, and uses it to {@link TypeAdapter#write write} the given field.
	 *
	 * <p>
	 * This is the basic, canonical way to write fields, but usually we can optimize
	 * this by looking up the {@link TypeAdapter} ahead of time, while compiling the
	 * codec, so we can save the overhead of the lookup operation during serialization.
	 */
	@SuppressWarnings({"rawtypes","unchecked"})
	protected static void dynamicWriteField(Object fieldValue, String fieldName, TypeToken<?> typeToken, Gson gson, JsonWriter out) throws IOException {
		TypeAdapter adapter = gson.getAdapter(typeToken);
		out.name(fieldName);
		adapter.write(out, fieldValue);
	}

}
