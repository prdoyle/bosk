package io.vena.bosk.gson;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.vena.bosk.Bosk;
import io.vena.bosk.Catalog;
import io.vena.bosk.Entity;
import io.vena.bosk.Identifier;
import io.vena.bosk.ListValue;
import io.vena.bosk.Listing;
import io.vena.bosk.ListingEntry;
import io.vena.bosk.MapValue;
import io.vena.bosk.Path;
import io.vena.bosk.Phantom;
import io.vena.bosk.Reference;
import io.vena.bosk.SerializationPlugin;
import io.vena.bosk.SideTable;
import io.vena.bosk.StateTreeNode;
import io.vena.bosk.exceptions.InvalidTypeException;
import io.vena.bosk.exceptions.UnexpectedPathException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static io.vena.bosk.ListingEntry.LISTING_ENTRY;
import static io.vena.bosk.ReferenceUtils.parameterType;
import static io.vena.bosk.ReferenceUtils.rawClass;
import static io.vena.bosk.ReferenceUtils.theOnlyConstructorFor;
import static java.util.Objects.requireNonNull;

/**
 * Provides JSON serialization/deserialization using Gson.
 * @see SerializationPlugin
 */
public final class GsonPlugin extends SerializationPlugin {
	private final GsonAdapterCompiler compiler = new GsonAdapterCompiler(this);

	// Java's generics are just not capable of the following shenanigans.
	// This method leaps on the generics grenade so most of this class can
	// benefit from solid type checking.
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <R extends Entity> TypeAdapterFactory adaptersFor(Bosk<R> bosk) {
		return new TypeAdapterFactory() {
			@Override
			public TypeAdapter create(Gson gson, TypeToken typeToken) {
				TypeAdapter result = getTypeAdapter(gson, typeToken);
				// While we don't have any particular desire to tolerate nulls here,
				// the Gson docs require it.
				// See: https://github.com/google/gson/blob/ceae88bd6667f4263bbe02e6b3710b8a683906a2/gson/src/main/java/com/google/gson/TypeAdapter.java#L146
				return (result == null) ? null : result.nullSafe();
			}

			private TypeAdapter getTypeAdapter(Gson gson, TypeToken typeToken) {
				Class theClass = typeToken.getRawType();
				if (Catalog.class.isAssignableFrom(theClass)) {
					return catalogAdapter(gson, typeToken);
				} else if (Listing.class.isAssignableFrom(theClass)) {
					return listingAdapter(gson);
				} else if (Reference.class.isAssignableFrom(theClass)) {
					return referenceAdapter(bosk);
				} else if (Identifier.class.isAssignableFrom(theClass)) {
					return identifierAdapter();
				} else if (ListingEntry.class.isAssignableFrom(theClass)) {
					return listingEntryAdapter();
				} else if (SideTable.class.isAssignableFrom(theClass)) {
					return sideTableAdapter(gson, typeToken);
				} else if (StateTreeNode.class.isAssignableFrom(theClass)) {
					return stateTreeNodeAdapter(gson, typeToken, bosk);
				} else if (Optional.class.isAssignableFrom(theClass)) {
					// Optional.empty() can't be serialized on its own because the field name itself must also be omitted
					throw new IllegalArgumentException("Cannot serialize an Optional on its own; only as a field of another object");
				} else if (Phantom.class.isAssignableFrom(theClass)) {
					throw new IllegalArgumentException("Cannot serialize a Phantom on its own; only as a field of another object");
				} else if (ListValue.class.isAssignableFrom(theClass)) {
					return listValueAdapter(gson, typeToken);
				} else if (MapValue.class.isAssignableFrom(theClass)) {
					return mapValueAdapter(gson, typeToken);
				} else {
					return null;
				}
			}

		};
	}

	private <V> TypeAdapter<ListValue<V>> listValueAdapter(Gson gson, TypeToken<ListValue<V>> typeToken) {
		Constructor<?> ctor = theOnlyConstructorFor(typeToken.getRawType());
		TypeToken<V[]> arrayType = listValueEquivalentArrayTypeToken(typeToken);
		TypeAdapter<V[]> arrayTypeAdapter = gson.getAdapter(arrayType);
		return new TypeAdapter<ListValue<V>>() {
			@Override
			public void write(JsonWriter out, ListValue<V> value) throws IOException {
				arrayTypeAdapter.write(out, value.toArray());
			}

			@SuppressWarnings("unchecked")
			@Override
			public ListValue<V> read(JsonReader in) throws IOException {
				V[] elementArray = arrayTypeAdapter.read(in);
				try {
					return (ListValue<V>) ctor.newInstance((Object) elementArray);
				} catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
					throw new IOException("Failed to instantiate " + typeToken.getRawType().getSimpleName() + ": " + e.getMessage(), e);
				}
			}
		};
	}

	private <V> TypeAdapter<MapValue<V>> mapValueAdapter(Gson gson, TypeToken<MapValue<V>> typeToken) {
		@SuppressWarnings("unchecked")
		TypeToken<V> valueTypeToken = (TypeToken<V>) TypeToken.get(parameterType(typeToken.getType(), MapValue.class, 0));
		TypeAdapter<V> valueAdapter = gson.getAdapter(valueTypeToken);
		return new TypeAdapter<MapValue<V>>() {
			@Override
			public void write(JsonWriter out, MapValue<V> value) throws IOException {
				out.beginObject();
				for (Entry<String, V> element: value.entrySet()) {
					out.name(requireNonNull(element.getKey()));
					valueAdapter.write(out, requireNonNull(element.getValue()));
				}
				out.endObject();
			}

			@Override
			public MapValue<V> read(JsonReader in) throws IOException {
				LinkedHashMap<String, V> result = new LinkedHashMap<>();
				in.beginObject();
				while (in.hasNext()) {
					String key = in.nextName();
					V value = valueAdapter.read(in);
					V old = result.put(requireNonNull(key), requireNonNull(value));
					if (old != null) {
						throw new JsonParseException("MapValue key appears twice: \"" + key + "\"");
					}
				}
				in.endObject();
				return MapValue.fromOrderedMap(result);
			}
		};
	}

	private TypeAdapter<Reference<?>> referenceAdapter(Bosk<?> bosk) {
		return new TypeAdapter<Reference<?>>() {
			@Override
			public void write(JsonWriter out, Reference<?> ref) throws IOException {
				out.value(ref.path().urlEncoded());
			}

			@Override
			public Reference<?> read(JsonReader in) throws IOException {
				try {
					return bosk.reference(Object.class, Path.parse(in.nextString()));
				} catch (InvalidTypeException e) {
					throw new UnexpectedPathException(e);
				}
			}
		};
	}

	private <E extends Entity> TypeAdapter<Listing<E>> listingAdapter(Gson gson) {
		TypeAdapter<Reference<Catalog<E>>> referenceAdapter = gson.getAdapter(new TypeToken<Reference<Catalog<E>>>() {});
		TypeAdapter<List<Identifier>> idListAdapter = gson.getAdapter(ID_LIST_TOKEN);
		return new TypeAdapter<Listing<E>>() {
			@Override
			public void write(JsonWriter out, Listing<E> listing) throws IOException {
				out.beginObject();

				out.name("ids");
				idListAdapter.write(out, new ArrayList<>(listing.ids()));

				out.name("domain");
				referenceAdapter.write(out, listing.domain());

				out.endObject();
			}

			@Override
			public Listing<E> read(JsonReader in) throws IOException {
				Reference<Catalog<E>> domain = null;
				List<Identifier> ids = null;

				in.beginObject();

				while (in.hasNext()) {
					String fieldName = in.nextName();
					switch (fieldName) {
						case "ids":
							if (ids == null) {
								ids = idListAdapter.read(in);
							} else {
								throw new JsonParseException("'ids' field appears twice");
							}
							break;
						case "domain":
							if (domain == null) {
								domain = referenceAdapter.read(in);
							} else {
								throw new JsonParseException("'domain' field appears twice");
							}
							break;
						default:
							throw new JsonParseException("Unrecognized field in Listing: " + fieldName);
					}
				}

				in.endObject();

				if (domain == null) {
					throw new JsonParseException("Missing 'domain' field");
				} else if (ids == null) {
					throw new JsonParseException("Missing 'ids' field");
				} else {
					return Listing.of(domain, ids);
				}
			}
		};
	}

	private <K extends Entity, V> TypeAdapter<SideTable<K,V>> sideTableAdapter(Gson gson, TypeToken<SideTable<K,V>> typeToken) {
		TypeToken<V> valueToken = sideTableValueTypeToken(typeToken);
		TypeAdapter<Reference<Catalog<K>>> referenceAdapter = gson.getAdapter(new TypeToken<Reference<Catalog<K>>>() {});
		TypeAdapter<V> valueAdapter = gson.getAdapter(valueToken);
		return new TypeAdapter<SideTable<K,V>>() {
			@Override
			public void write(JsonWriter out, SideTable<K,V> sideTable) throws IOException {
				out.beginObject();

				out.name("valuesById");
				writeMapEntries(out, sideTable.idEntrySet(), valueAdapter);

				out.name("domain");
				referenceAdapter.write(out, sideTable.domain());

				out.endObject();
			}

			@Override
			public SideTable<K,V> read(JsonReader in) throws IOException {
				Reference<Catalog<K>> domain = null;
				LinkedHashMap<Identifier, V> valuesById = null;

				in.beginObject();

				while (in.hasNext()) {
					String fieldName = in.nextName();
					switch (fieldName) {
						case "valuesById":
							if (valuesById == null) {
								valuesById = readMapEntries(in, valueAdapter);
							} else {
								throw new JsonParseException("'valuesById' field appears twice");
							}
							break;
						case "domain":
							if (domain == null) {
								domain = referenceAdapter.read(in);
							} else {
								throw new JsonParseException("'domain' field appears twice");
							}
							break;
						default:
							throw new JsonParseException("Unrecognized field in SideTable: " + fieldName);
					}
				}

				in.endObject();

				if (domain == null) {
					throw new JsonParseException("Missing 'domain' field");
				} else if (valuesById == null) {
					throw new JsonParseException("Missing 'valuesById' field");
				} else {
					return SideTable.fromOrderedMap(domain, valuesById);
				}
			}

		};
	}

	private <V> void writeMapEntries(JsonWriter out, Iterable<Entry<Identifier, V>> entries, TypeAdapter<V> valueAdapter) throws IOException {
		out.beginArray();
		for (Entry<Identifier, V> entry: entries) {
			out.beginObject();
			out.name(entry.getKey().toString());
			valueAdapter.write(out, entry.getValue());
			out.endObject();
		}
		out.endArray();
	}

	private <V> LinkedHashMap<Identifier, V> readMapEntries(JsonReader in, TypeAdapter<V> valueAdapter) throws IOException {
		LinkedHashMap<Identifier, V> result = new LinkedHashMap<>();
		in.beginArray();
		while (in.hasNext()) {
			in.beginObject();
			String fieldName = in.nextName();
			V value = valueAdapter.read(in);
			in.endObject();

			V oldValue = result.put(Identifier.from(fieldName), value);
			if (oldValue != null) {
				throw new JsonParseException("Duplicate sideTable entry '" + fieldName + "'");
			}
		}
		in.endArray();
		return result;
	}

	private <E extends Entity> TypeAdapter<Catalog<E>> catalogAdapter(Gson gson, TypeToken<Catalog<E>> typeToken) {
		TypeToken<E> typeParameterToken = catalogEntryTypeToken(typeToken);
		TypeAdapter<E> elementAdapter = gson.getAdapter(typeParameterToken);

		return new TypeAdapter<Catalog<E>>() {
			@Override
			public void write(JsonWriter out, Catalog<E> catalog) throws IOException {
				writeMapEntries(out, catalog.asMap().entrySet(), elementAdapter);
			}

			@Override
			public Catalog<E> read(JsonReader in) throws IOException {
				return Catalog.of(readMapEntries(in, elementAdapter).values());
			}
		};
	}

	private static final TypeToken<List<Identifier>> ID_LIST_TOKEN = new TypeToken<List<Identifier>>() {};

	private TypeAdapter<Identifier> identifierAdapter() {
		return new TypeAdapter<Identifier>() {
			@Override
			public void write(JsonWriter out, Identifier id) throws IOException {
				out.value(id.toString());
			}

			@Override
			public Identifier read(JsonReader in) throws IOException {
				return Identifier.from(in.nextString());
			}
		};
	}

	private TypeAdapter<ListingEntry> listingEntryAdapter() {
		// We serialize ListingEntry as a boolean `true` with the following rationale:
		// - The only "unit type" in JSON is null
		// - `null` is not suitable because many systems treat that as being equivalent to an absent field
		// - Of the other types, boolean seems the most likely to be efficiently processed in every system
		// - `false` gives the wrong impression
		// Hence, by a process of elimination, `true` it is

		return new TypeAdapter<ListingEntry>() {
			@Override
			public void write(JsonWriter out, ListingEntry entry) throws IOException {
				out.value(true);
			}

			@Override
			public ListingEntry read(JsonReader in) throws IOException {
				boolean result = in.nextBoolean();
				if (result) {
					return LISTING_ENTRY;
				} else {
					throw new JsonParseException("Unexpected Listing entry value: " + result);
				}
			}
		};
	}

	private <N extends StateTreeNode> TypeAdapter<N> stateTreeNodeAdapter(Gson gson, TypeToken<N> typeToken, Bosk<?> bosk) {
		return compiler.compiled(typeToken, bosk, gson);
	}

	//
	// Helpers
	//

	/**
	 * Returns the fields present in the JSON, with value objects deserialized
	 * using type information from <code>parametersByName</code>.
	 */
	public Map<String, Object> gatherParameterValuesByName(Class<?> nodeClass, Map<String, Parameter> parametersByName, JsonReader in, Gson gson) throws IOException {
		Map<String, Object> parameterValuesByName = new HashMap<>();
		while (in.hasNext()) {
			String name = in.nextName();
			Parameter parameter = parametersByName.get(name);
			if (parameter == null) {
				throw new JsonParseException("No such parameter in constructor for " + nodeClass.getSimpleName() + ": " + name);
			} else {
				Type parameterType = parameter.getParameterizedType();
				Object value = readField(name, in, gson, parameterType);
				Object prev = parameterValuesByName.put(name, value);
				if (prev != null) {
					throw new JsonParseException("Parameter appeared twice: " + name);
				}
			}
		}
		return parameterValuesByName;
	}

	private Object readField(String name, JsonReader in, Gson gson, Type parameterType) throws IOException {
		// TODO: Combine with similar method in BsonPlugin
		Class<?> effectiveClass = rawClass(parameterType);
		if (Optional.class.isAssignableFrom(effectiveClass)) {
			// Optional field is present in JSON; wrap deserialized value in Optional.of
			Type contentsType = parameterType(parameterType, Optional.class, 0);
			Object deserializedValue = readField(name, in, gson, contentsType);
			return Optional.of(deserializedValue);
		} else if (Phantom.class.isAssignableFrom(effectiveClass)) {
			throw new JsonParseException("Unexpected phantom field \"" + name + "\"");
		} else {
			TypeAdapter<?> parameterAdapter = gson.getAdapter(TypeToken.get(parameterType));
			return parameterAdapter.read(in);
		}
	}

	@SuppressWarnings("unchecked")
	private static <EE extends Entity> TypeToken<EE> catalogEntryTypeToken(TypeToken<Catalog<EE>> typeToken) {
		return (TypeToken<EE>) TypeToken.get(tokenParameterType(typeToken, Catalog.class, 0));
	}

	@SuppressWarnings("unchecked")
	private static <K extends Entity, V> TypeToken<V> sideTableValueTypeToken(TypeToken<SideTable<K,V>> typeToken) {
		return (TypeToken<V>) TypeToken.get(tokenParameterType(typeToken, SideTable.class, 1));
	}

	@SuppressWarnings("unchecked")
	private static <V> TypeToken<V[]> listValueEquivalentArrayTypeToken(TypeToken<ListValue<V>> typeToken) {
		return (TypeToken<V[]>) TypeToken.getArray(parameterType(typeToken.getType(), ListValue.class, 0));
	}

	private static Type tokenParameterType(TypeToken<?> typeToken, Class<?> expectedClass, int index) {
		return parameterType(typeToken.getType(), expectedClass, index);
	}

}
