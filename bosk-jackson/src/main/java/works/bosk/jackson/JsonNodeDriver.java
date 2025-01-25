package works.bosk.jackson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import works.bosk.BoskDriver;
import works.bosk.BoskInfo;
import works.bosk.DriverFactory;
import works.bosk.Entity;
import works.bosk.Identifier;
import works.bosk.Reference;
import works.bosk.StateTreeNode;
import works.bosk.exceptions.InvalidTypeException;
import works.bosk.jackson.JsonNodeFinder.NodeInfo;
import works.bosk.jackson.JsonNodeFinder.NodeLocation.NonexistentParent;

/**
 * Maintains an in-memory representation of the bosk state
 * in the form of a tree of {@link JsonNode} objects.
 */
public class JsonNodeDriver implements BoskDriver {
	final BoskDriver downstream;
	final ObjectMapper mapper;
	final JsonNodeFinder finder;
	protected JsonNode currentRoot;
	int updateNumber = 0;

	public static <R extends StateTreeNode> DriverFactory<R> factory(JacksonPlugin jacksonPlugin) {
		return (b,d) -> new JsonNodeDriver(b, d, jacksonPlugin);
	}

	protected JsonNodeDriver(BoskInfo<?> bosk, BoskDriver downstream, JacksonPlugin jacksonPlugin) {
		this.downstream = downstream;
		this.mapper = new ObjectMapper();
		this.finder = new JsonNodeFinder();
		mapper.registerModule(jacksonPlugin.moduleFor(bosk));
	}

	@Override
	public synchronized StateTreeNode initialRoot(Type rootType) throws InvalidTypeException, IOException, InterruptedException {
		StateTreeNode result = downstream.initialRoot(rootType);
		currentRoot = mapper.convertValue(result, JsonNode.class);
		traceCurrentState("After initialRoot");
		return result;
	}

	@Override
	public synchronized <T> void submitReplacement(Reference<T> target, T newValue) {
		traceCurrentState("Before submitReplacement");
		doReplacement(finder.nodeInfo(currentRoot, target), target.path().lastSegment(), newValue);
		downstream.submitReplacement(target, newValue);
		traceCurrentState("After submitReplacement");
	}

	@Override
	public synchronized <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		traceCurrentState("Before submitConditionalReplacement");
		if (requiredValue.toString().equals(finder.node(currentRoot, precondition).textValue())) {
			doReplacement(finder.nodeInfo(currentRoot, target), target.path().lastSegment(), newValue);
		}
		downstream.submitConditionalReplacement(target, newValue, precondition, requiredValue);
		traceCurrentState("After submitConditionalReplacement");
	}

	@Override
	public synchronized <T> void submitInitialization(Reference<T> target, T newValue) {
		traceCurrentState("Before submitInitialization");
		if (finder.node(currentRoot, target) == null) {
			doReplacement(finder.nodeInfo(currentRoot, target), target.path().lastSegment(), newValue);
		}
		downstream.submitInitialization(target, newValue);
		traceCurrentState("After submitInitialization");
	}

	@Override
	public synchronized <T> void submitDeletion(Reference<T> target) {
		traceCurrentState("Before submitDeletion");
		doDeletion(finder.nodeInfo(currentRoot, target));
		downstream.submitDeletion(target);
		traceCurrentState("After submitDeletion");
	}

	@Override
	public synchronized <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		traceCurrentState("Before submitConditionalDeletion");
		if (requiredValue.toString().equals(finder.node(currentRoot, precondition).textValue())) {
			doDeletion(finder.nodeInfo(currentRoot, target));
		}
		downstream.submitConditionalDeletion(target, precondition, requiredValue);
		traceCurrentState("After submitConditionalDeletion");
	}

	@Override
	public synchronized void flush() throws IOException, InterruptedException {
		traceCurrentState("Before flush");
		downstream.flush();
	}

	private <T> void doReplacement(NodeInfo nodeInfo, String lastSegment, T newValue) {
		var location = nodeInfo.replacementLocation();
		if (location instanceof NonexistentParent) {
			// Nothing to do
			return;
		}
		JsonNode replacement = switch (nodeInfo.replacementStyle()) {
			case PLAIN -> mapper.convertValue(newValue, JsonNode.class);
			case ID_ONLY -> new TextNode(lastSegment);
			case WRAPPED_ENTITY -> new ObjectNode(JsonNodeFactory.instance, Map.of(((Entity) newValue).id().toString(), mapper.convertValue(newValue, JsonNode.class)));
		};
		switch (location) {
			case JsonNodeFinder.NodeLocation.Root() -> {
				currentRoot = replacement;
			}
			case JsonNodeFinder.NodeLocation.ArrayElement(var parent, int i) -> {
				if (parent.size() == i) {
					parent.add(replacement);
				} else {
					parent.set(i, replacement);
				}
			}
			case JsonNodeFinder.NodeLocation.ObjectMember(var parent, String member) -> {
				parent.set(member, replacement);
			}
			case NonexistentParent() -> {
				throw new AssertionError("This should already have been handled");
			}
		}
	}

	private <T> void doDeletion(NodeInfo nodeInfo) {
		switch (nodeInfo.replacementLocation()) {
			case JsonNodeFinder.NodeLocation.Root() -> {
				throw new IllegalStateException("Cannot delete root");
			}
			case JsonNodeFinder.NodeLocation.ArrayElement(var parent, int i) -> {
				parent.remove(i);
			}
			case JsonNodeFinder.NodeLocation.ObjectMember(var parent, String member) -> {
				parent.remove(member);
			}
			case NonexistentParent() -> {
				// Nothing to do
			}
		}
	}

	void traceCurrentState(String description) {
		if (LOGGER.isTraceEnabled()) {
			try {
				LOGGER.trace("State {} {}:\n{}", ++updateNumber, description, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(currentRoot));
			} catch (JsonProcessingException e) {
				throw new IllegalStateException(e);
			}
		}
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(JsonNodeDriver.class);
}
