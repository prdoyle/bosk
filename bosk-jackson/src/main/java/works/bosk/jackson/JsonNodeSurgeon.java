package works.bosk.jackson;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.Map;
import java.util.function.Supplier;
import works.bosk.Catalog;
import works.bosk.Listing;
import works.bosk.Path;
import works.bosk.Reference;
import works.bosk.SideTable;
import works.bosk.exceptions.NotYetImplementedException;
import works.bosk.jackson.JsonNodeSurgeon.NodeLocation.ArrayElement;
import works.bosk.jackson.JsonNodeSurgeon.NodeLocation.NonexistentParent;
import works.bosk.jackson.JsonNodeSurgeon.NodeLocation.ObjectMember;
import works.bosk.jackson.JsonNodeSurgeon.NodeLocation.Root;

import static java.util.Objects.requireNonNull;
import static works.bosk.jackson.JsonNodeSurgeon.ReplacementStyle.ID_ONLY;
import static works.bosk.jackson.JsonNodeSurgeon.ReplacementStyle.PLAIN;
import static works.bosk.jackson.JsonNodeSurgeon.ReplacementStyle.WRAPPED_ENTITY;

/**
 * Utilities to find and modify the {@link JsonNode} corresponding to
 * a given bosk {@link Reference}.
 *
 * <p>
 * Note: currently only works with {@link works.bosk.jackson.JacksonPluginConfiguration.MapShape#ARRAY MapShape.ARRAY}.
 */
public class JsonNodeSurgeon {
	/**
	 * Describes how to access a particular {@link JsonNode} from its parent node.
	 */
	public sealed interface NodeLocation {
		public record Root() implements NodeLocation {}
		public record ObjectMember(ObjectNode parent, String memberName) implements NodeLocation {
			public ObjectMember {
				requireNonNull(parent);
				requireNonNull(memberName);
			}
		}
		public record ArrayElement(ArrayNode parent, int elementIndex) implements NodeLocation {
			public ArrayElement {
				requireNonNull(parent);
				// Note that elementIndex == parent.size() is valid for nonexistent array elements
				if (elementIndex < 0 || elementIndex > parent.size()) {
					throw new IllegalArgumentException("Invalid index: " + elementIndex);
				}
			}
		}

		/**
		 * The parent {@link JsonNode} does not exist, so there's no way to describe
		 * the desired node's location.
		 *
		 * <p>
		 * (Note that the parent {@link JsonNode} is not always the node corresponding
		 * to the {@link Reference#enclosingReference enclosing reference}.
		 * They're two related but different concepts.)
		 */
		public record NonexistentParent() implements NodeLocation {}
	}

	/**
	 * Describes what should be placed at a {@link NodeLocation} in order to
	 * {@link works.bosk.BoskDriver#submitReplacement replace} a bosk node.
	 */
	public enum ReplacementStyle {
		/**
		 * Use the serialized form of the resired value
		 */
		PLAIN,

		/**
		 * Use the {@link works.bosk.Entity#id id} of the desired entity
		 */
		ID_ONLY,

		/**
		 * Use an {@link ObjectNode} having a single member whose name is the desired
		 * entity's {@link works.bosk.Entity#id id} and whose value is the serialized
		 * form of the desired entity.
		 *
		 * <p>
		 * (Note that this is the style that causes the {@link NodeInfo#valueLocation value location}
		 * to differ from the {@link NodeInfo#replacementLocation replacement location},
		 * because the value is placed inside another object.)
		 */
		WRAPPED_ENTITY,
	};

	/**
	 * Information about which {@link JsonNode} corresponds to a particular {@link Reference}.
	 * This can be used to find the node, and to replace or delete it.
	 *
	 * <p>
	 * The indicated {@link JsonNode} need not actually exist in the tree: so long as the
	 * parent node exists, we can describe where the node <em>would</em> be if it existed,
	 * which is useful for adding new nodes to the tree as well as modifying existing ones.
	 *
	 * <p>
	 * However, if the parent node does not exist, the location will be described using {@link NonexistentParent NonexistentParent}.
	 *
	 * @param valueLocation the JsonNode containing the referenced value
	 * @param replacementLocation the JsonNode to be replaced when the reference value is modified
	 * @param replacementStyle the means by which {@code replacementLocation} should be modified
	 */
	public record NodeInfo(
		NodeLocation valueLocation,
		NodeLocation replacementLocation,
		ReplacementStyle replacementStyle
	) {
		static NodeInfo plain(NodeLocation theLocation) {
			return new NodeInfo(theLocation, theLocation, PLAIN);
		}

		static NodeInfo idOnly(NodeLocation theLocation) {
			return new NodeInfo(theLocation, theLocation, ID_ONLY);
		}

		static NodeInfo wrappedEntity(NodeLocation valueLocation, NodeLocation replacementLocation) {
			return new NodeInfo(valueLocation, replacementLocation, WRAPPED_ENTITY);
		}
	}

	/**
	 * @return null if {@code doc} has no node corresponding to {@code ref}
	 */
	public JsonNode node(JsonNode doc, Reference<?> ref) {
		return getNode(nodeInfo(doc, ref).valueLocation, doc);
	}

	/**
	 * @return the JSON node that must be modified to replace or delete {@code ref}.
	 */
	public NodeInfo nodeInfo(JsonNode doc, Reference<?> ref) {
		if (ref.isRoot()) {
			return NodeInfo.plain(new Root());
		}

		// For some kinds of enclosing nodes, the JSON structure differs from the bosk structure.
		// Peel off those cases.
		Reference<?> enclosingRef = ref.enclosingReference(Object.class);
		NodeLocation parentLocation = nodeInfo(doc, enclosingRef).valueLocation;
		var parent = getNode(parentLocation, doc);
		if (Listing.class.isAssignableFrom(enclosingRef.targetClass())) {
			if (parent == null) {
				return NodeInfo.idOnly(new NonexistentParent());
			}
			var ids = (ArrayNode)parent.get("ids");
			var id = ref.path().lastSegment();
			for (int i = 0; i < ids.size(); i++) {
				if (id.equals(((TextNode)ids.get(i)).textValue())) {
					return NodeInfo.idOnly(new ArrayElement(ids, i));
				}
			}
			// New entries go at the end
			return NodeInfo.idOnly(new ArrayElement(ids, ids.size()));
		} else if (Catalog.class.isAssignableFrom(enclosingRef.targetClass())) {
			if (parent == null) {
				return NodeInfo.wrappedEntity(new NonexistentParent(), new NonexistentParent());
			} else {
				String entryID = ref.path().lastSegment();
				NodeLocation element = findArrayElementWithId(parent, entryID);
				JsonNode elementNode = getNode(element, doc);
				if (elementNode == null) {
					// Doesn't exist yet
					return NodeInfo.wrappedEntity (new NonexistentParent(), element);
				} else {
					return NodeInfo.wrappedEntity (getMapEntryValueLocation(elementNode, entryID), element);
				}
			}
		} else if (SideTable.class.isAssignableFrom(enclosingRef.targetClass())) {
			if (parent == null) {
				return NodeInfo.wrappedEntity(new NonexistentParent(), new NonexistentParent());
			} else {
				String entryID = ref.path().lastSegment();
				NodeLocation element = findArrayElementWithId(parent.get("valuesById"), entryID);
				JsonNode elementNode = getNode(element, doc);
				if (elementNode == null) {
					// Doesn't exist yet
					return NodeInfo.wrappedEntity(new NonexistentParent(), element);
				} else {
					return NodeInfo.wrappedEntity(getMapEntryValueLocation(elementNode, entryID), element);
				}
			}
		}
		
		// For every other type, this is an object member contained in the parent
		NodeLocation valueLocation = parent == null
			? new NonexistentParent()
			: new ObjectMember((ObjectNode) parent, ref.path().lastSegment());
		return NodeInfo.plain(valueLocation);
	}

	private static ObjectMember getMapEntryValueLocation(JsonNode element, String entryID) {
		return new ObjectMember((ObjectNode) element, entryID);
	}

	private static NodeLocation findArrayElementWithId(JsonNode entries, String id) {
		ArrayNode entriesArray = (ArrayNode) entries;
		for (int i = 0; i < entriesArray.size(); i++) {
			ObjectNode entryObject = (ObjectNode) entries.get(i);
			var properties = entryObject.properties();
			if (properties.size() != 1) {
				throw new NotYetImplementedException(properties.toString());
			}
			Map.Entry<String, JsonNode> entry = properties.iterator().next();
			if (id.equals(entry.getKey())) {
				return new ArrayElement(entriesArray, i);
			}
		}
		return new ArrayElement(entriesArray, entries.size());
	}

	private static JsonNode getNode(NodeLocation nodeLocation, JsonNode rootDocument) {
		return switch (nodeLocation) {
			case ArrayElement a -> {
				yield a.parent().get(a.elementIndex());
			}
			case ObjectMember o -> {
				yield o.parent().get(o.memberName());
			}
			case Root() -> {
				yield rootDocument;
			}
			case NonexistentParent() -> {
				yield null;
			}
		};
	}

	/**
	 * @throws IllegalArgumentException if {@link NodeInfo#replacementLocation() nodeInfo.replacementLocation} is {@link Root Root} or {@link NonexistentParent NonexistentParent}.
	 *                                  Callers must handle these cases.
	 */
	public void replaceNode(NodeInfo nodeInfo, JsonNode replacement) {
		var location = nodeInfo.replacementLocation();
		if (location instanceof NonexistentParent) {
			// Nothing to do
			return;
		}
		switch (location) {
			case Root() -> {
				throw new IllegalArgumentException("Cannot replace root node");
			}
			case JsonNodeSurgeon.NodeLocation.ArrayElement(var parent, int i) -> {
				if (parent.size() == i) {
					parent.add(replacement);
				} else {
					parent.set(i, replacement);
				}
			}
			case JsonNodeSurgeon.NodeLocation.ObjectMember(var parent, String member) -> {
				parent.set(member, replacement);
			}
			case NonexistentParent() -> {
				throw new IllegalArgumentException("This should already have been handled");
			}
		}
	}

	/**
	 * @param lastSegment the {@link Path#lastSegment() lastSegment} of the {@link Reference} pointing at the node to be replaced.
	 */
	public JsonNode replacementNode(NodeInfo nodeInfo, String lastSegment, Supplier<JsonNode> newValue) {
		return switch (nodeInfo.replacementStyle()) {
			case PLAIN -> newValue.get();
			case ID_ONLY -> new TextNode(lastSegment);
			case WRAPPED_ENTITY -> {
				JsonNode entityNode = newValue.get();
				String id = entityNode.get("id").textValue();
				yield new ObjectNode(JsonNodeFactory.instance, Map.of(id, entityNode));
			}
		};
	}

	public void deleteNode(NodeInfo nodeInfo) {
		switch (nodeInfo.replacementLocation()) {
			case Root() -> {
				throw new IllegalArgumentException("Cannot delete root");
			}
			case JsonNodeSurgeon.NodeLocation.ArrayElement(var parent, int i) -> {
				parent.remove(i);
			}
			case JsonNodeSurgeon.NodeLocation.ObjectMember(var parent, String member) -> {
				parent.remove(member);
			}
			case NonexistentParent() -> {
				// Nothing to do
			}
		}
	}

}
