package io.vena.bosk;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.Deserializers;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.Serializers;
import com.fasterxml.jackson.databind.type.ArrayType;
import com.fasterxml.jackson.databind.type.TypeBindings;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.vena.bosk.annotations.DerivedRecord;
import io.vena.bosk.codecs.JacksonAdapterCompiler;
import io.vena.bosk.exceptions.InvalidTypeException;
import io.vena.bosk.exceptions.TunneledCheckedException;
import io.vena.bosk.exceptions.UnexpectedPathException;
import java.io.IOException;
import java.lang.reflect.Array;
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
import lombok.Value;

import static io.vena.bosk.ListingEntry.LISTING_ENTRY;
import static io.vena.bosk.ReferenceUtils.parameterType;
import static io.vena.bosk.ReferenceUtils.rawClass;
import static io.vena.bosk.ReferenceUtils.theOnlyConstructorFor;
import static java.util.Objects.requireNonNull;

public final class JacksonPlugin extends SerializationPlugin {
	private final JacksonAdapterCompiler compiler = new JacksonAdapterCompiler(this);

	public Module moduleFor(Bosk<?> bosk) {
		return new Module() {
			@Override
			public String getModuleName() {
				return JacksonPlugin.class.getSimpleName();
			}

			@Override
			public Version version() {
				return Version.unknownVersion();
			}

			@Override
			public void setupModule(SetupContext context) {
				context.addSerializers(new BoskSerializers(bosk));
				context.addDeserializers(new BoskDeserializers(bosk));
			}
		};
	}

	private static final class BoskSerializers extends Serializers.Base {
		private final Bosk<?> bosk;

		public BoskSerializers(Bosk<?> bosk) {
			this.bosk = bosk;
		}

		@Override
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public JsonSerializer<?> findSerializer(SerializationConfig config, JavaType type, BeanDescription beanDesc) {
			Class theClass = type.getRawClass();
			if (theClass.isAnnotationPresent(DerivedRecord.class)) {
				return derivedRecordSerDes(type, config, beanDesc, bosk).serializer();
			} else if (Catalog.class.isAssignableFrom(theClass)) {
				return catalogSerDes(type, config, beanDesc, bosk).serializer();
			} else if (Listing.class.isAssignableFrom(theClass)) {
				return listingSerDes(type, config, beanDesc, bosk).serializer();
			} else if (Reference.class.isAssignableFrom(theClass)) {
				return referenceSerDes(type, config, beanDesc, bosk).serializer();
			} else if (Identifier.class.isAssignableFrom(theClass)) {
				return identifierSerDes(type, config, beanDesc, bosk).serializer();
			} else if (ListingEntry.class.isAssignableFrom(theClass)) {
				return listingEntrySerDes(type, config, beanDesc, bosk).serializer();
			} else if (SideTable.class.isAssignableFrom(theClass)) {
				return sideTableSerDes(type, config, beanDesc, bosk).serializer();
			} else if (StateTreeNode.class.isAssignableFrom(theClass)) {
				return stateTreeNodeSerDes(type, config, beanDesc, bosk).serializer();
			} else if (Optional.class.isAssignableFrom(theClass)) {
				// Optional.empty() can't be serialized on its own because the field name itself must also be omitted
				throw new IllegalArgumentException("Cannot serialize an Optional on its own; only as a field of another object");
			} else if (Phantom.class.isAssignableFrom(theClass)) {
				throw new IllegalArgumentException("Cannot serialize a Phantom on its own; only as a field of another object");
			} else if (ListValue.class.isAssignableFrom(theClass)) {
				return listValueSerDes(type, config, beanDesc, bosk).serializer();
			} else if (MapValue.class.isAssignableFrom(theClass)) {
				return mapValueSerDes(type, config, beanDesc, bosk).serializer();
			} else {
				return null;
			}
		}
	}

	private static final class BoskDeserializers extends Deserializers.Base {
		private final Bosk<?> bosk;

		public <R extends Entity> BoskDeserializers(Bosk<?> bosk) {
			this.bosk = bosk;
		}

		@Override
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public JsonDeserializer<?> findBeanDeserializer(JavaType type, DeserializationConfig config, BeanDescription beanDesc) throws JsonMappingException {
			Class theClass = type.getRawClass();
			if (theClass.isAnnotationPresent(DerivedRecord.class)) {
				return derivedRecordDeserializer(type, config, beanDesc, bosk);
			} else if (Catalog.class.isAssignableFrom(theClass)) {
				return catalogDeserializer(type, config, beanDesc, bosk);
			} else if (Listing.class.isAssignableFrom(theClass)) {
				return listingDeserializer(type, config, beanDesc, bosk);
			} else if (Reference.class.isAssignableFrom(theClass)) {
				return referenceDeserializer(type, config, beanDesc, bosk);
			} else if (Identifier.class.isAssignableFrom(theClass)) {
				return identifierDeserializer(type, config, beanDesc, bosk);
			} else if (ListingEntry.class.isAssignableFrom(theClass)) {
				return listingEntryDeserializer(type, config, beanDesc, bosk);
			} else if (SideTable.class.isAssignableFrom(theClass)) {
				return sideTableDeserializer(type, config, beanDesc, bosk);
			} else if (StateTreeNode.class.isAssignableFrom(theClass)) {
				return stateTreeNodeDeserializer(type, config, beanDesc, bosk);
			} else if (Optional.class.isAssignableFrom(theClass)) {
				// Optional.empty() can't be serialized on its own because the field name itself must also be omitted
				throw new IllegalArgumentException("Cannot serialize an Optional on its own; only as a field of another object");
			} else if (Phantom.class.isAssignableFrom(theClass)) {
				throw new IllegalArgumentException("Cannot serialize a Phantom on its own; only as a field of another object");
			} else if (ListValue.class.isAssignableFrom(theClass)) {
				return listValueDeserializer(type, config, beanDesc, bosk);
			} else if (MapValue.class.isAssignableFrom(theClass)) {
				return mapValueDeserializer(type, config, beanDesc, bosk);
			} else {
				return null;
			}
		}
	}

	private interface SerDes<T> {
		JsonSerializer<T> serializer();
		JsonDeserializer<T> deserializer();
	}

	private <V> SerDes<ListValue<V>> listValueSerDes(JavaType type, SerializationConfig serializationConfig, DeserializationConfig deserializationConfig, BeanDescription beanDesc, Bosk<?> bosk) {
		Constructor<?> ctor = theOnlyConstructorFor(type.getRawClass());
		JavaType elementType = type.getBindings().getBoundType(0);
		JavaType arrayType = ArrayType.construct(elementType, TypeBindings.emptyBindings());
		return new SerDes<ListValue<V>>() {
			@Override
			public JsonSerializer<ListValue<V>> serializer() {
				return new JsonSerializer<ListValue<V>>() {
					@Override
					public void serialize(ListValue<V> value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
						serializers.findValueSerializer(arrayType, null)
							.serialize(value.toArray(), gen, serializers);
					}
				};
			}

			@Override
			public JsonDeserializer<ListValue<V>> deserializer() {
				return new JsonDeserializer<ListValue<V>>() {
					@Override
					@SuppressWarnings({"unchecked"})
					public ListValue<V> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
						JsonDeserializer<Object> foo = ctxt.findContextualValueDeserializer(arrayType, null);
						Object elementArray = (V[]) foo.deserialize(p, ctxt);
						try {
							return (ListValue<V>) ctor.newInstance(elementArray);
						} catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
							throw new IOException("Failed to instantiate " + type.getRawClass().getSimpleName() + ": " + e.getMessage(), e);
						}
					}
				};
			}
		};
	}

	private <V> SerDes<MapValue<V>> mapValueAdapter(JavaType type, SerializationConfig serializationConfig, DeserializationConfig deserializationConfig, BeanDescription beanDesc, Bosk<?> bosk) {
		JavaType valueType = type.getBindings().getBoundType(1);
		return new SerDes<MapValue<V>>() {
			@Override
			public JsonSerializer<MapValue<V>> serializer() {
				return new JsonSerializer<MapValue<V>>() {
					@Override
					public void serialize(MapValue<V> value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
						JsonSerializer<Object> valueSerializer = serializers.findValueSerializer(valueType);
						gen.writeStartObject();
						for (Entry<String, V> element: value.entrySet()) {
							gen.writeFieldName(requireNonNull(element.getKey()));
							valueSerializer.serialize(requireNonNull(element.getValue()), gen, serializers);
						}
						gen.writeEndObject();
					}
				};
			}

			@Override
			public JsonDeserializer<MapValue<V>> deserializer() {
				return new JsonDeserializer<MapValue<V>>() {
					@Override
					public MapValue<V> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
						LinkedHashMap<String, V> result = new LinkedHashMap<>();
						JsonToken token;
						if (p.nextToken() == JsonToken.START_OBJECT) {
							for (String key = p.nextFieldName(); key != null; key = p.nextFieldName()) {
								@SuppressWarnings("unchecked")
								V value = (V)ctxt.findContextualValueDeserializer(valueType, null);
								V old = result.put(key, value);
								if (old != null) {
									throw new JsonParseException(p, "MapValue key appears twice: \"" + key + "\"");
								}
							}
							if (p.nextToken() != JsonToken.END_OBJECT) {
								throw new JsonParseException(p, "Expected end of object for " + type);
							}
							return MapValue.fromOrderedMap(result);
						} else {
							throw new JsonParseException(p, "Expected object for " + type);
						}
					}
				};
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
			V value;
			try (@SuppressWarnings("unused") DeserializationScope scope = innerDeserializationScope(fieldName)) {
				value = valueAdapter.read(in);
			}
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
		StateTreeNodeFieldModerator moderator = new StateTreeNodeFieldModerator(typeToken.getType());
		return compiler.compiled(typeToken, bosk, gson, moderator);
	}

	private <T> TypeAdapter<T> derivedRecordAdapter(Gson gson, TypeToken<T> typeToken, Bosk<?> bosk) {
		Type objType = typeToken.getType();

		// Check for special cases
		Class<?> objClass = rawClass(objType);
		if (ListValue.class.isAssignableFrom(objClass)) { // TODO: MapValue?
			Class<?> entryClass = rawClass(parameterType(objType, ListValue.class, 0));
			if (ReflectiveEntity.class.isAssignableFrom(entryClass)) {
				@SuppressWarnings("unchecked")
				TypeAdapter<T> result = derivedRecordListValueOfReflectiveEntityAdapter(gson, objType, objClass, entryClass);
				return result;
			} else if (Entity.class.isAssignableFrom(entryClass)) {
				throw new IllegalArgumentException("Can't hold non-reflective Entity type in @" + DerivedRecord.class.getSimpleName() + " " + objType);
			}
		}

		// Default DerivedRecord handling
		DerivedRecordFieldModerator moderator = new DerivedRecordFieldModerator(objType);
		return compiler.compiled(typeToken, bosk, gson, moderator);
	}


	@SuppressWarnings({ "rawtypes", "unchecked" })
	private <E extends ReflectiveEntity<E>, L extends ListValue<E>> TypeAdapter derivedRecordListValueOfReflectiveEntityAdapter(Gson gson, Type objType, Class objClass, Class entryClass) {
		Constructor<L> constructor = (Constructor<L>) theOnlyConstructorFor(objClass);
		Class<?>[] parameters = constructor.getParameterTypes();
		if (parameters.length == 1 && parameters[0].getComponentType().equals(entryClass)) {
			TypeToken<Reference<E>> elementType = (TypeToken)TypeToken.getParameterized(Reference.class, entryClass);
			TypeAdapter<Reference<E>> elementAdapter = gson.getAdapter(elementType);
			return new TypeAdapter<L>() {
				@Override
				public void write(JsonWriter out, L value) throws IOException {
					out.beginArray();
					try {
						value.forEach(entry -> {
							try {
								elementAdapter.write(out, entry.reference());
							} catch (IOException e) {
								throw new TunneledCheckedException(e);
							}
						});
					} catch (TunneledCheckedException e) {
						throw e.getCause(IOException.class);
					}
					out.endArray();
				}

				@Override
				public L read(JsonReader in) throws IOException {
					in.beginArray();
					List<E> entries = new ArrayList<>();
					while (in.hasNext()) {
						entries.add(elementAdapter.read(in).value());
					}
					in.endArray();

					E[] array = (E[])Array.newInstance(entryClass, entries.size());
					try {
						return constructor.newInstance(new Object[] { entries.toArray(array) } );
					} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
						throw new IOException("Error creating " + objClass.getSimpleName() + ": " + e.getMessage(), e);
					}
				}
			};
		} else {
			throw new IllegalArgumentException("Cannot serialize " + ListValue.class.getSimpleName() + " subtype " + objType
					+ ": constructor must have a single array parameter of type " + entryClass.getSimpleName() + "[]");
		}
	}

	/**
	 * Allows custom logic for the serialization and deserialization of an
	 * object's fields (actually its constructor parameters).
	 *
	 * @author Patrick Doyle
	 */
	public interface FieldModerator {
		Type typeOf(Type parameterType);
		Object valueFor(Type parameterType, Object deserializedValue);
	}

	/**
	 * The "normal" {@link FieldModerator} that doesn't add any extra logic.
	 *
	 * @author Patrick Doyle
	 */
	@Value
	private static class StateTreeNodeFieldModerator implements FieldModerator {
		Type nodeType;

		@Override
		public Type typeOf(Type parameterType) {
			return parameterType;
		}

		@Override
		public Object valueFor(Type parameterType, Object deserializedValue) {
			return deserializedValue;
		}

	}

	/**
	 * Performs additional serialization logic for {@link DerivedRecord}
	 * objects. Specifically {@link ReflectiveEntity} fields, serializes them as
	 * though they were {@link Reference}s; otherwise, serializes normally.
	 *
	 * @author Patrick Doyle
	 */
	@Value
	private static class DerivedRecordFieldModerator implements FieldModerator {
		Type nodeType;

		@Override
		public Type typeOf(Type parameterType) {
			if (reflectiveEntity(parameterType)) {
				// These are serialized as References
				return ReferenceUtils.referenceTypeFor(parameterType);
			} else {
				return parameterType;
			}
		}

		@Override
		public Object valueFor(Type parameterType, Object deserializedValue) {
			if (reflectiveEntity(parameterType)) {
				// The deserialized value is a Reference; what we want is Reference.value()
				return ((Reference<?>)deserializedValue).value();
			} else {
				return deserializedValue;
			}
		}

		private boolean reflectiveEntity(Type parameterType) {
			Class<?> parameterClass = rawClass(parameterType);
			if (ReflectiveEntity.class.isAssignableFrom(parameterClass)) {
				return true;
			} else if (Entity.class.isAssignableFrom(parameterClass)) {
				throw new IllegalArgumentException(DerivedRecord.class.getSimpleName() + " " + rawClass(nodeType).getSimpleName() + " cannot contain " + Entity.class.getSimpleName() + " that is not a " + ReflectiveEntity.class.getSimpleName() + ": " + parameterType);
			} else if (Catalog.class.isAssignableFrom(parameterClass)) {
				throw new IllegalArgumentException(DerivedRecord.class.getSimpleName() + " " + rawClass(nodeType).getSimpleName() + " cannot contain Catalog (try Listing)");
			} else {
				return false;
			}
		}

	}

	//
	// Helpers
	//

	/**
	 * Returns the fields present in the JSON, with value objects deserialized
	 * using type information from <code>parametersByName</code>.
	 */
	public Map<String, Object> gatherParameterValuesByName(Class<?> nodeClass, Map<String, Parameter> parametersByName, FieldModerator moderator, JsonReader in, Gson gson) throws IOException {
		Map<String, Object> parameterValuesByName = new HashMap<>();
		while (in.hasNext()) {
			String name = in.nextName();
			Parameter parameter = parametersByName.get(name);
			if (parameter == null) {
				throw new JsonParseException("No such parameter in constructor for " + nodeClass.getSimpleName() + ": " + name);
			} else {
				Type parameterType = parameter.getParameterizedType();
				Object deserializedValue;
				try (@SuppressWarnings("unused") DeserializationScope scope = nodeFieldDeserializationScope(nodeClass, name)) {
					deserializedValue = readField(name, in, gson, parameterType, moderator);
				}
				Object value = moderator.valueFor(parameterType, deserializedValue);
				Object prev = parameterValuesByName.put(name, value);
				if (prev != null) {
					throw new JsonParseException("Parameter appeared twice: " + name);
				}
			}
		}
		return parameterValuesByName;
	}

	private Object readField(String name, JsonReader in, Gson gson, Type parameterType, FieldModerator moderator) throws IOException {
		// TODO: Combine with similar method in BsonPlugin
		Type effectiveType = moderator.typeOf(parameterType);
		Class<?> effectiveClass = rawClass(effectiveType);
		if (Optional.class.isAssignableFrom(effectiveClass)) {
			// Optional field is present in JSON; wrap deserialized value in Optional.of
			Type contentsType = parameterType(effectiveType, Optional.class, 0);
			Object deserializedValue = readField(name, in, gson, contentsType, moderator);
			return Optional.of(deserializedValue);
		} else if (Phantom.class.isAssignableFrom(effectiveClass)) {
			throw new JsonParseException("Unexpected phantom field \"" + name + "\"");
		} else {
			TypeAdapter<?> parameterAdapter = gson.getAdapter(TypeToken.get(effectiveType));
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