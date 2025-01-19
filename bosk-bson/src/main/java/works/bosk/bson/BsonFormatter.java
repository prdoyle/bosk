package works.bosk.bson;

import java.lang.reflect.Type;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonReader;
import org.bson.BsonValue;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import works.bosk.BoskInfo;
import works.bosk.Listing;
import works.bosk.Reference;
import works.bosk.SerializationPlugin;
import works.bosk.SideTable;

import static works.bosk.ReferenceUtils.rawClass;

public class BsonFormatter {
	protected static final UnaryOperator<String> DECODER;
	protected static final UnaryOperator<String> ENCODER;
	protected final CodecRegistry simpleCodecs;
	protected final Function<Type, Codec<?>> preferredBoskCodecs;
	protected final Function<Reference<?>, SerializationPlugin.DeserializationScope> deserializationScopeFunction;

	public BsonFormatter(BoskInfo<?> boskInfo, BsonPlugin bsonPlugin) {
		this.simpleCodecs = CodecRegistries.fromProviders(
			bsonPlugin.codecProviderFor(boskInfo),
			new ValueCodecProvider(),
			new DocumentCodecProvider());
		this.preferredBoskCodecs = type -> bsonPlugin.getCodec(type, rawClass(type), simpleCodecs, boskInfo);
		this.deserializationScopeFunction = bsonPlugin::newDeserializationScope;
	}

	/**
	 * @param refLength behave as though <code>ref</code> were truncated to this length without actually having to do it
	 */
	private static <T> void buildDottedFieldNameOf(Reference<T> ref, int startLength, int refLength, ArrayList<String> segments) {
		if (ref.path().length() > startLength) {
			Reference<?> enclosingReference = ref.enclosingReference(Object.class);
			BsonFormatter.buildDottedFieldNameOf(enclosingReference, startLength, refLength, segments);
			if (ref.path().length() <= refLength) {
				if (Listing.class.isAssignableFrom(enclosingReference.targetClass())) {
					segments.add("ids");
				} else if (SideTable.class.isAssignableFrom(enclosingReference.targetClass())) {
					segments.add("valuesById");
				}
				segments.add(BsonFormatter.dottedFieldNameSegment(ref.path().lastSegment()));
			}
		}
	}

	public static String dottedFieldNameSegment(String segment) {
		return ENCODER.apply(segment);
	}

	public static String undottedFieldNameSegment(String dottedSegment) {
		return DECODER.apply(dottedSegment);
	}

	static {
		DECODER = s->{
			return URLDecoder.decode(s, StandardCharsets.UTF_8);
		};

		ENCODER = s->{
			// Selective percent-encoding of characters MongoDB doesn't like.
			// Standard percent-encoding doesn't handle the period character, which
			// we want, so if we're already diverging from the standard, we might
			// as well do something that suits our needs.
			// Good to stay compatible with standard percent-DEcoding, though.
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < s.length(); ) {
				int cp = s.codePointAt(i);
				switch (cp) {
					case '%': // For percent-encoding
					case '+': case ' ': // These two are affected by URLDecoder
					case '$': // MongoDB treats these specially
					case '.': // MongoDB separator for dotted field names
					case 0:   // Can MongoDB handle nulls? Probably. Do we want to find out? Not really.
					case '|': // (These are reserved for internal use)
					case '!':
					case '~':
					case '[':
					case ']':
						appendPercentEncoded(sb, cp);
						break;
					default:
						sb.appendCodePoint(cp);
						break;
				}
				i += Character.charCount(cp);
			}
			return sb.toString();
		};
	}

	private static void appendPercentEncoded(StringBuilder sb, int cp) {
		assert 0 <= cp && cp <= 255;
		sb
			.append('%')
			.append(hexCharForDigit(cp / 16))
			.append(hexCharForDigit(cp % 16));
	}

	/**
	 * An uppercase version of {@link Character#forDigit} with a radix of 16.
	 */
	private static char hexCharForDigit(int value) {
		if (value < 10) {
			return (char)('0' + value);
		} else {
			return (char)('A' + value - 10);
		}
	}

	protected Codec<?> codecFor(Type type) {
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
	public <T> T bsonValue2object(BsonValue bson, Reference<T> target) {
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

	/**
	 * The fields of the main MongoDB document.  Case-sensitive.
	 *
	 * <p>
	 * No field name should be a prefix of any other.
	 */
	public enum DocumentFields {
		path,
		state,
		revision,
		diagnostics,
	}

	public static <T> ArrayList<String> dottedFieldNameSegments(Reference<T> ref, int refLength, Reference<?> startingRef) {
		assert startingRef.path().matchesPrefixOf(ref.path()): "'" + ref + "' must be under '" + startingRef + "'";
		ArrayList<String> segments = new ArrayList<>();
		segments.add(DocumentFields.state.name());
		buildDottedFieldNameOf(ref, startingRef.path().length(), refLength, segments);
		return segments;
	}

	/**
	 * @param elementRefLength behave as though <code>elementRef</code> were truncated to this length, without actually having to do it
	 * @return MongoDB field name corresponding to the container (Catalog or SideTable) that contains the given element
	 * @see #referenceTo(String, Reference)
	 */
	static <T> List<String> containerSegments(Reference<T> elementRef, int elementRefLength, Reference<?> startingRef) {
		List<String> elementSegments = dottedFieldNameSegments(elementRef, elementRefLength, startingRef);
		return elementSegments.subList(0, elementSegments.size()-1); // Trim off the element itself
	}

}
