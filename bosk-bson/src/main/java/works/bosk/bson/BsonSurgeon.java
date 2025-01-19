package works.bosk.bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInvalidOperationException;
import org.bson.BsonString;
import org.bson.BsonValue;
import works.bosk.Bosk;
import works.bosk.EnumerableByIdentifier;
import works.bosk.Identifier;
import works.bosk.Path;
import works.bosk.Reference;
import works.bosk.SideTable;
import works.bosk.bson.BsonSurgeonFormatter.DocumentFields;
import works.bosk.exceptions.InvalidTypeException;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static works.bosk.bson.BsonSurgeonFormatter.containerSegments;
import static works.bosk.bson.BsonSurgeonFormatter.dottedFieldNameSegments;
import static works.bosk.bson.BsonSurgeonFormatter.undottedFieldNameSegment;

/**
 * Splits up a single large BSON document into multiple self-describing pieces,
 * and re-assembles them. Provides the core mechanism to carve large BSON structures
 * into pieces so they can stay under the MongoDB document size limit.
 * <p>
 *
 * Jargon:
 * <dl>
 *     <dt>Root document</dt>
 *     <dd>BSON document corresponding to the bosk root reference</dd>
 *     <dt>Main document</dt>
 *     <dd>BSON document corresponding to the part of the state tree being described</dd>
 * </dl>
 */
public class BsonSurgeon {
	final List<GraftPoint> graftPoints;

	public record GraftPoint (
		Reference<? extends EnumerableByIdentifier<?>> containerRef,
		Reference<?> entryPlaceholderRef
	) {
		public static GraftPoint of(Reference<? extends EnumerableByIdentifier<?>> containerRef, Reference<?> entryPlaceholderRef) {
			return new GraftPoint(containerRef, entryPlaceholderRef);
		}

		public GraftPoint boundTo(Identifier id) {
			return GraftPoint.of(
				containerRef.boundTo(id),
				entryPlaceholderRef.boundTo(id)
			);
		}
	}

	public List<GraftPoint> graftPoints() {
		return unmodifiableList(graftPoints);
	}

	/**
	 * We put the whole path in _id so that it will be present in change stream documents
	 */
	private static final String BSON_PATH_FIELD = "_id";

	private static final String STATE_FIELD = DocumentFields.state.name();

	public BsonSurgeon(List<Reference<? extends EnumerableByIdentifier<?>>> graftPoints) {
		this.graftPoints = new ArrayList<>(graftPoints.size());
		graftPoints.stream()
			// Scatter bottom-up so we don't have to worry about scattering already-scattered documents
			.sorted(comparing((Reference<?> ref) -> ref.path().length()).reversed())
			.forEachOrdered(containerRef -> this.graftPoints.add(
				GraftPoint.of(containerRef, entryRef(containerRef))));
	}

	static Reference<?> entryRef(Reference<? extends EnumerableByIdentifier<?>> containerRef) {
		// We need a reference pointing all the way to the collection entry, so that if the
		// collection itself has BSON fields (like SideTable does), those fields will be included
		// in the dotted name segment list. The actual ID we pick doesn't matter and will be ignored.
		String entryID = "SURGEON_PLACEHOLDER";
		try {
			return containerRef.then(Object.class, entryID);
		} catch (InvalidTypeException e) {
			// Could conceivably happen if a user created their own type that extends EnumerableByIdentifier.
			// The built-in subtypes (Catalog, SideTable) won't cause this problem.
			throw new IllegalArgumentException("Error constructing entry reference from \"" + containerRef + "\" of type " + containerRef.targetType(), e);
		}
	}

	/**
	 * For efficiency, this modifies <code>document</code> in-place.
	 *
	 * @param docRef   the bosk node corresponding to <code>document</code>
	 * @param document will be modified!
	 * @param rootRef  {@link Bosk#rootReference()}
	 * @return list of {@link BsonDocument}s which, when passed to {@link #gather}, combine to form the original <code>document</code>.
	 * The main document, document corresponding to <code>docRef</code>, will be at the end of the list.
	 * @see #gather
	 */
	public List<BsonDocument> scatter(Reference<?> docRef, BsonDocument document, Reference<?> rootRef) {
		List<BsonDocument> parts = new ArrayList<>();
		for (GraftPoint graftPoint: graftPoints) {
			scatterOneCollection(docRef, graftPoint, document, rootRef, parts);
		}

		// `document` has now had the scattered pieces replaced by BsonBoolean.TRUE
		String docBsonPath = "|" + String.join("|", docSegments(docRef, rootRef));
		parts.add(createRecipe(document, docBsonPath));

		return parts;
	}

	/**
	 * @return list of field names suitable for {@link #lookup} to find the document corresponding
	 * to <code>docRef</code> inside a document corresponding to <code>rootRef</code>
	 */
	public static List<String> docSegments(Reference<?> docRef, Reference<?> rootRef) {
		ArrayList<String> allSegments = dottedFieldNameSegments(docRef, docRef.path().length(), rootRef);
		return allSegments
			.subList(1, allSegments.size()); // Skip the "state" field
	}

	private void scatterOneCollection(Reference<?> docRef, GraftPoint graftPoint, BsonDocument docToScatter, Reference<?> rootRef, List<BsonDocument> parts) {
		// Only continue if the graft could point to a proper descendant node of docRef
		Path graftPath = graftPoint.entryPlaceholderRef.path();
		Path docPath = docRef.path();
		if (graftPath.length() <= docPath.length()) {
			return;
		} else if (!docPath.matches(graftPath.truncatedTo(docPath.length()))) {
			return;
		}

		Reference<?> entryRef = graftPoint.entryPlaceholderRef.boundBy(docPath);
		Path entryRefPath = entryRef.path();
		if (entryRefPath.numParameters() == 0) {
			List<String> lookupSegments = containerDocSegments(entryRef, entryRef.path().length(), docRef);
			BsonDocument containerDoc = lookup(docToScatter, lookupSegments);
			String containerBsonPath = "|" + String.join("|",
				containerDocSegments(entryRef, entryRef.path().length(), rootRef)); // Bson paths are absolute
			for (Map.Entry<String, BsonValue> entry : containerDoc.entrySet()) {
				// Stub-out each entry in the container by replacing it with TRUE
				// and adding the actual contents to the parts list
				parts.add(createRecipe(entry.getValue(),
					containerBsonPath + "|" + entry.getKey()));
				entry.setValue(BsonBoolean.TRUE);
			}
		} else {
			// Loop through all possible values of the first parameter and recurse
			List<String> lookupSegments = containerDocSegments(entryRef, entryRefPath.firstParameterIndex() + 1, docRef);
			BsonDocument containerDoc = lookup(docToScatter, lookupSegments);
			containerDoc.forEach((fieldName, value) -> {
				Identifier entryID = Identifier.from(undottedFieldNameSegment(fieldName));
				scatterOneCollection(
					docRef, graftPoint.boundTo(entryID), docToScatter, rootRef,
					parts);
			});
		}
	}

	private static List<String> containerDocSegments(Reference<?> docRef, int docRefLength, Reference<?> entryRef) {
		List<String> allSegments = containerSegments(docRef, docRefLength, entryRef);
		return allSegments.subList(1, allSegments.size()); // Remove "state" segment
	}

	/**
	 * <code>entryPath</code> and <code>entryBsonPath</code> must correspond to each other.
	 * They'll have the same segments, except where the BSON representation of a container actually contains its own
	 * fields (as with {@link SideTable}, in which case those fields will appear too.
	 */
	private static BsonDocument createRecipe(BsonValue entryState, String bsonPathString) {
		return new BsonDocument()
			.append(BSON_PATH_FIELD, new BsonString(bsonPathString))
			.append(STATE_FIELD, entryState);
	}

	private static List<String> bsonPathSegments(BsonString bsonPath) {
		assert bsonPath.getValue().startsWith("|"): "bsonPath \"" + bsonPath + "\" must start with vertical bar";
		String bsonPathString = bsonPath.getValue().substring(1);
		if (bsonPathString.isEmpty()) {
			// String.split doesn't do the right thing in this case. We want an empty array,
			// not an array containing a single empty string.
			return emptyList();
		} else {
			return Arrays.asList(bsonPathString.split("\\|"));
		}
	}

	private static BsonDocument lookup(BsonDocument entireDoc, List<String> segments) {
		BsonDocument result = entireDoc;
		for (String segment: segments) {
			try {
				result = result.getDocument(segment);
			} catch (BsonInvalidOperationException e) {
				throw new IllegalArgumentException("Doc does not contain " + segments, e);
			}
		}
		return result;
	}

	/**
	 * For efficiency, this modifies <code>partsList</code> in-place.
	 *
	 * <p>
	 * <code>partsList</code> is a list of "instructions" for assembling a larger document.
	 * By design, this method is supposed to be simple and general;
	 * any sophistication should be in {@link #scatter}.
	 * This way, {@link #scatter} can evolve without breaking backward compatibility
	 * with parts lists from existing databases.
	 *
	 * <p>
	 * This method's behaviour is not sensitive to the ordering of <code>partsList</code>.
	 *
	 * @param partsList will be modified!
	 * @see #scatter
	 */
	public BsonDocument gather(List<BsonDocument> partsList) {
		// Sorting by path length ensures we gather parents before children.
		// (Sorting lexicographically might be better for cache locality.)
		partsList.sort(comparing(doc -> doc.getString(BSON_PATH_FIELD).getValue().length()));

		Set<BsonString> alreadySeen = new HashSet<>();

		BsonDocument rootRecipe = partsList.get(0);
		List<String> prefix = bsonPathSegments(rootRecipe.getString(BSON_PATH_FIELD));

		BsonDocument whole = rootRecipe.getDocument(STATE_FIELD);
		for (BsonDocument entry: partsList.subList(1, partsList.size())) {
			BsonString bsonPath = entry.getString(BSON_PATH_FIELD);
			if (!alreadySeen.add(bsonPath)) {
				throw new IllegalArgumentException("Duplicate path \"" + bsonPath.getValue() + "\"");
			}
			List<String> bsonSegments = bsonPathSegments(bsonPath);
			if (!bsonSegments.subList(0, prefix.size()).equals(prefix)) {
				throw new IllegalArgumentException("Part doc is not contained within the root doc. Part: " + bsonSegments + " Root:" + prefix);
			}
			String key = bsonSegments.get(bsonSegments.size()-1);
			BsonValue value = requireNonNull(entry.get(STATE_FIELD));

			// The container should already have an entry. We'll be replacing it,
			// and this does not affect the order of the entries.
			BsonDocument container = lookup(whole, bsonSegments.subList(prefix.size(), bsonSegments.size() - 1));
			container.put(key, value);
		}

		return whole;
	}

}
