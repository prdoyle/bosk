package works.bosk.drivers.mongo;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.UpdateDescription;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import lombok.NonNull;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonReader;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonValueCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BasicOutputBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import works.bosk.BoskInfo;
import works.bosk.Listing;
import works.bosk.MapValue;
import works.bosk.Reference;
import works.bosk.SerializationPlugin;
import works.bosk.SideTable;
import works.bosk.bson.BsonPlugin;
import works.bosk.bson.BsonSurgeonFormatter;
import works.bosk.exceptions.InvalidTypeException;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static works.bosk.ReferenceUtils.rawClass;

/**
 * Facilities to translate between in-DB and in-memory representations.
 *
 * @author pdoyle
 */
public final class Formatter extends BsonSurgeonFormatter {
	private final CodecRegistry simpleCodecs;
	private final Function<Type, Codec<?>> preferredBoskCodecs;
	private final Function<Reference<?>, SerializationPlugin.DeserializationScope> deserializationScopeFunction;

	/**
	 * If the diagnostic attributes are identical from one update to the next,
	 * MongoDB won't send them. This field retains the last value so we can
	 * always set the correct context for each downstream operation.
	 */
	private volatile MapValue<String> lastEventDiagnosticAttributes = MapValue.empty();

	public Formatter(BoskInfo<?> boskInfo, BsonPlugin bsonPlugin) {
		this.simpleCodecs = CodecRegistries.fromProviders(
			bsonPlugin.codecProviderFor(boskInfo),
			new ValueCodecProvider(),
			new DocumentCodecProvider());
		this.preferredBoskCodecs = type -> bsonPlugin.getCodec(type, rawClass(type), simpleCodecs, boskInfo);
		this.deserializationScopeFunction = bsonPlugin::newDeserializationScope;
	}

	/**
	 * Revision number zero represents a nonexistent version number,
	 * consistent with the behaviour of the MongoDB <code>$inc</code> operator,
	 * which treats a nonexistent field as a zero.
	 * For us, this occurs in two situations:
	 *
	 * <ol><li>
	 *     Initialization: There is no state information yet because the document doesn't exist
	 * </li><li>
	 *     Legacy: The database collection pre-dates revision numbers and doesn't have one
	 * </li></ol>
	 *
	 * The revision field in the database is never less than this value: it is initialized to
	 * {@link #REVISION_ONE} and incremented thereafter. Therefore, waiting for a revision
	 * greater than or equal to this is equivalent to waiting for any update at all.
	 */
	static final BsonInt64 REVISION_ZERO = new BsonInt64(0);

	/**
	 * The revision number used when the bosk document is first created.
	 */
	static final BsonInt64 REVISION_ONE = new BsonInt64(1);

	private final BsonInt32 SUPPORTED_MANIFEST_VERSION = new BsonInt32(1);

	//
	// Helpers to translate Bosk <-> MongoDB
	//

	Codec<?> codecFor(Type type) {
		// BsonPlugin gives better codecs than CodecRegistry, because BsonPlugin is aware of generics,
		// so we always try that first. The CodecSupplier protocol uses "null" to indicate that another
		// CodecSupplier should be used, so we follow that protocol and fall back on the CodecRegistry.
		// TODO: Should this logic be in BsonPlugin? It has nothing to do with MongoDriver really.
		Codec<?> result = preferredBoskCodecs.apply(type);
		if (result == null) {
			return simpleCodecs.get(rawClass(type));
		} else {
			return result;
		}
	}

	@SuppressWarnings("unchecked")
	<T> T document2object(BsonDocument doc, Reference<T> target) {
		Type type = target.targetType();
		Class<T> objectClass = (Class<T>) rawClass(type);
		Codec<T> objectCodec = (Codec<T>) codecFor(type);
		try (@SuppressWarnings("unused") SerializationPlugin.DeserializationScope scope = deserializationScopeFunction.apply(target)) {
			return objectClass.cast(objectCodec.decode(doc.asBsonReader(), DecoderContext.builder().build()));
		}
	}

	void validateManifest(BsonDocument manifest) throws UnrecognizedFormatException {
		try {
			Set<String> keys = new HashSet<>(manifest.keySet());
			List<String> supportedFormats = asList("sequoia", "pando");
			String detectedFormat = null;
			for (String format: supportedFormats) {
				if (keys.remove(format)) {
					if (detectedFormat == null) {
						detectedFormat = format;
					} else {
						throw new UnrecognizedFormatException("Found two supported formats: " + detectedFormat + " and " + format);
					}
				}
			}
			if (detectedFormat == null) {
				throw new UnrecognizedFormatException("Found none of the supported formats: " + supportedFormats);
			}
			HashSet<String> requiredKeys = new HashSet<>(singletonList("version"));
			if (!keys.equals(requiredKeys)) {
				keys.removeAll(requiredKeys);
				if (keys.isEmpty()) {
					requiredKeys.removeAll(manifest.keySet());
					throw new UnrecognizedFormatException("Missing keys in manifest: " + requiredKeys);
				} else {
					throw new UnrecognizedFormatException("Unrecognized keys in manifest: " + keys);
				}
			}
			if (!SUPPORTED_MANIFEST_VERSION.equals(manifest.getInt32("version"))) {
				throw new UnrecognizedFormatException("Manifest version " + manifest.getInt32("version") + " not suppoted");
			}
		} catch (ClassCastException e) {
			throw new UnrecognizedFormatException("Manifest field has unexpected type", e);
		}
	}

	Manifest decodeManifest(BsonDocument manifestDoc) throws UnrecognizedFormatException {
		BsonDocument manifest = manifestDoc.clone();
		manifest.remove("_id");
		validateManifest(manifest);
		return (Manifest) codecFor(Manifest.class)
			.decode(
				new BsonDocumentReader(manifest),
				DecoderContext.builder().build());
	}

	BsonDocument encodeDiagnostics(MapValue<String> attributes) {
		BsonDocument result = new BsonDocument();
		attributes.forEach((name, value) -> result.put(name, new BsonString(value)));
		return new BsonDocument("attributes", result);
	}

	MapValue<String> decodeDiagnosticAttributes(BsonDocument diagnostics) {
		MapValue<String> result = MapValue.empty();
		for (Map.Entry<String, BsonValue> foo: diagnostics.getDocument("attributes").entrySet()) {
			String name = foo.getKey();
			String value = foo.getValue().asString().getValue();
			result = result.with(name, value);
		}
		return result;
	}

	/**
	 * @see #bsonValue2object(BsonValue, Reference)
	 */
	@SuppressWarnings("unchecked")
	public <T> BsonValue object2bsonValue(T object, Type type) {
		rawClass(type).cast(object);
		Codec<T> objectCodec = (Codec<T>) codecFor(type);
		BsonDocument document = new BsonDocument();
		try (BsonDocumentWriter writer = new BsonDocumentWriter(document)) {
			// To support arbitrary values, not just whole documents, we put the result INSIDE a document.
			writer.writeStartDocument();
			writer.writeName("value");
			objectCodec.encode(writer, object, EncoderContext.builder().build());
			writer.writeEndDocument();
		}
		return document.get("value");
	}

	/**
	 * @see #object2bsonValue(Object, Type)
	 */
	@SuppressWarnings("unchecked")
	<T> T bsonValue2object(BsonValue bson, Reference<T> target) {
		Codec<T> objectCodec = (Codec<T>) codecFor(target.targetType());
		BsonDocument document = new BsonDocument();
		document.append("value", bson);
		try (
			@SuppressWarnings("unused") SerializationPlugin.DeserializationScope scope = deserializationScopeFunction.apply(target);
			BsonReader reader = document.asBsonReader()
		) {
			reader.readStartDocument();
			reader.readName("value");
			return objectCodec.decode(reader, DecoderContext.builder().build());
		}
	}

	@SuppressWarnings("unchecked")
	long bsonValueBinarySize(BsonValue bson) {
		Codec<BsonValue> codec = new BsonValueCodec();
		try (
			BasicOutputBuffer buffer = new BasicOutputBuffer();
			BsonBinaryWriter w = new BsonBinaryWriter(buffer)
		) {
			codec.encode(w, bson, EncoderContext.builder().build());
			return buffer.getPosition();
		}
	}

	BsonInt64 getRevisionFromFullDocument(BsonDocument fullDocument) {
		if (fullDocument == null) {
			return null;
		}
		return fullDocument.getInt64(DocumentFields.revision.name(), null);
	}

	@NonNull MapValue<String> eventDiagnosticAttributesFromFullDocument(BsonDocument fullDocument) {
		return getOrSetEventDiagnosticAttributes(getDiagnosticAttributesFromFullDocument(fullDocument));
	}

	MapValue<String> getDiagnosticAttributesFromFullDocument(BsonDocument fullDocument) {
		BsonDocument diagnostics = getDiagnosticAttributesIfAny(fullDocument);
		return diagnostics == null ? null : decodeDiagnosticAttributes(diagnostics);
	}

	BsonInt64 getRevisionFromUpdateEvent(ChangeStreamDocument<BsonDocument> event) {
		BsonDocument updatedFields = getUpdatedFieldsIfAny(event);
		if (updatedFields == null) {
			return null;
		}
		return updatedFields.getInt64(DocumentFields.revision.name(), null);
	}

	@NonNull MapValue<String> eventDiagnosticAttributesFromUpdate(ChangeStreamDocument<BsonDocument> event) {
		return getOrSetEventDiagnosticAttributes(getDiagnosticAttributesFromUpdateEvent(event));
	}

	MapValue<String> getDiagnosticAttributesFromUpdateEvent(ChangeStreamDocument<BsonDocument> event) {
		BsonDocument updatedFields = getUpdatedFieldsIfAny(event);
		BsonDocument diagnostics = getDiagnosticAttributesIfAny(updatedFields);
		return diagnostics == null ? null : decodeDiagnosticAttributes(diagnostics);
	}

	private static BsonDocument getUpdatedFieldsIfAny(ChangeStreamDocument<BsonDocument> event) {
		if (event == null) {
			return null;
		}
		UpdateDescription updateDescription = event.getUpdateDescription();
		if (updateDescription == null) {
			return null;
		}
		return updateDescription.getUpdatedFields();
	}

	static BsonDocument getDiagnosticAttributesIfAny(BsonDocument fullDocument) {
		if (fullDocument == null) {
			return null;
		}
		return fullDocument.getDocument(DocumentFields.diagnostics.name(), null);
	}

	@NonNull private MapValue<String> getOrSetEventDiagnosticAttributes(MapValue<String> fromEvent) {
		if (fromEvent == null) {
			LOGGER.debug("No diagnostic attributes in event; assuming they are unchanged");
			return lastEventDiagnosticAttributes;
		} else {
			lastEventDiagnosticAttributes = fromEvent;
			return fromEvent;
		}
	}

	/**
	 * @return MongoDB field name corresponding to the given Reference
	 * @see #referenceTo(String, Reference)
	 */
	static <T> String dottedFieldNameOf(Reference<T> ref, Reference<?> startingRef) {
		return dottedFieldNameOf(ref, ref.path().length(), startingRef);
	}

	/**
	 * @param refLength behave as though <code>ref</code> were truncated to this length, without actually having to do it
	 * @return MongoDB field name corresponding to the given Reference
	 * @see #referenceTo(String, Reference)
	 */
	static <T> String dottedFieldNameOf(Reference<T> ref, int refLength, Reference<?> startingRef) {
		ArrayList<String> segments = dottedFieldNameSegments(ref, refLength, startingRef);
		return String.join(".", segments.toArray(new String[0]));
	}

	/**
	 * @return Reference corresponding to the given field name
	 * @see #dottedFieldNameOf
	 */
	@SuppressWarnings("unchecked")
	static <T> Reference<T> referenceTo(String dottedName, Reference<?> startingReference) throws InvalidTypeException {
		Reference<?> ref = startingReference;
		Iterator<String> iter = Arrays.asList(dottedName.split(Pattern.quote("."))).iterator();
		skipField(ref, iter, DocumentFields.state.name()); // The entire Bosk state is in this field
		while (iter.hasNext()) {
			if (Listing.class.isAssignableFrom(ref.targetClass())) {
				skipField(ref, iter, "ids");
			} else if (SideTable.class.isAssignableFrom(ref.targetClass())) {
				skipField(ref, iter, "valuesById");
			}
			if (iter.hasNext()) {
				String segment = undottedFieldNameSegment(iter.next());
				ref = ref.then(Object.class, segment);
			}
		}
		return (Reference<T>) ref;
	}

	private static void skipField(Reference<?> ref, Iterator<String> iter, String expectedName) {
		String actualName;
		try {
			actualName = iter.next();
		} catch (NoSuchElementException e) {
			throw new IllegalStateException("Expected '" + expectedName + "' for " + ref.targetClass().getSimpleName() + "; encountered end of dotted field name");
		}
		if (!expectedName.equals(actualName)) {
			throw new IllegalStateException("Expected '" + expectedName + "' for " + ref.targetClass().getSimpleName() + "; was: " + actualName);
		}
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(Formatter.class);
}
