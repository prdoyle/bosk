package works.bosk.jackson;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.SortedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;
import works.bosk.BoskContext;
import works.bosk.BoskContext.Tenant.Established;
import works.bosk.BoskDriver;
import works.bosk.BoskDriver.InitialState.MultiTree;
import works.bosk.BoskDriver.InitialState.SingleTree;
import works.bosk.BoskInfo;
import works.bosk.DriverFactory;
import works.bosk.Identifier;
import works.bosk.Reference;
import works.bosk.StateTreeNode;
import works.bosk.exceptions.InvalidTypeException;
import works.bosk.jackson.JsonNodeSurgeon.NodeInfo;
import works.bosk.jackson.JsonNodeSurgeon.NodeLocation.Root;

import static java.util.stream.Collectors.toMap;

/**
 * Maintains an in-memory representation of the bosk state
 * in the form of a tree of {@link JsonNode} objects.
 */
public class JsonNodeDriver implements BoskDriver {
	final BoskDriver downstream;
	final BoskContext context;
	final ObjectMapper mapper;
	final JsonNodeSurgeon surgeon;
	Contents contents; // Note: for multi-tree, this is actually mutable
	int updateNumber = 0;

	sealed interface Contents {
		JsonNode contents();
		record SingleTree(JsonNode root) implements Contents {
			@Override public JsonNode contents() { return root; }
		}
		record MultiTree(SortedMap<Established, JsonNode> roots) implements Contents {
			public void put(Established tenant, JsonNode root) {
				roots.put(tenant, root);
			}

			@Override
			public JsonNode contents() {
				var result = new ObjectNode(JsonNodeFactory.instance);
				roots.forEach((tenant, root) -> result.set(tenant.toString(), root));
				return result;
			}
		}
	}

	public static <R extends StateTreeNode> DriverFactory<R> factory(JacksonSerializer jacksonSerializer) {
		return (b,d) -> new JsonNodeDriver(b, d, jacksonSerializer);
	}

	protected JsonNodeDriver(BoskInfo<?> bosk, BoskDriver downstream, JacksonSerializer jacksonSerializer) {
		this.downstream = downstream;
		this.mapper = JsonMapper.builder()
			.addModule(jacksonSerializer.moduleFor(bosk))
			.build();
		this.surgeon = new JsonNodeSurgeon();
		this.context = bosk.context();
	}

	@Override
	public synchronized <R extends StateTreeNode> InitialState<R> initialState(Class<R> rootType) throws InvalidTypeException, IOException, InterruptedException {
		var result = downstream.initialState(rootType);
		contents = switch (result) {
			case SingleTree(var r) -> new Contents.SingleTree(mapper.convertValue(r, JsonNode.class));
			case MultiTree(var tenantRoots) -> new Contents.MultiTree(tenantRoots.entrySet().stream()
				.collect(toMap(
					Entry::getKey,
					e -> mapper.convertValue(e.getValue(), JsonNode.class),
					(_,b) -> b,
					java.util.TreeMap::new)));
		};
		traceCurrentState("After initialState");
		return result;
	}

	@Override
	public synchronized <T> void submitReplacement(Reference<T> target, T newValue) {
		traceCurrentState("Before submitReplacement");
		doReplacement(surgeon.nodeInfo(currentRoot(), target), target.path().lastSegment(), newValue);
		downstream.submitReplacement(target, newValue);
		traceCurrentState("After submitReplacement");
	}

	@Override
	public synchronized <T> void submitConditionalReplacement(Reference<T> target, T newValue, Reference<Identifier> precondition, Identifier requiredValue) {
		traceCurrentState("Before submitConditionalReplacement");
		if (requiredValue.toString().equals(surgeon.valueNode(currentRoot(), precondition).asString())) {
			doReplacement(surgeon.nodeInfo(currentRoot(), target), target.path().lastSegment(), newValue);
		}
		downstream.submitConditionalReplacement(target, newValue, precondition, requiredValue);
		traceCurrentState("After submitConditionalReplacement");
	}

	@Override
	public synchronized <T> void submitConditionalCreation(Reference<T> target, T newValue) {
		traceCurrentState("Before submitConditionalCreation");
		if (surgeon.valueNode(currentRoot(), target) == null) {
			doReplacement(surgeon.nodeInfo(currentRoot(), target), target.path().lastSegment(), newValue);
		}
		downstream.submitConditionalCreation(target, newValue);
		traceCurrentState("After submitConditionalCreation");
	}

	@Override
	public synchronized <T> void submitDeletion(Reference<T> target) {
		traceCurrentState("Before submitDeletion");
		surgeon.deleteNode(surgeon.nodeInfo(currentRoot(), target));
		downstream.submitDeletion(target);
		traceCurrentState("After submitDeletion");
	}

	@Override
	public synchronized <T> void submitConditionalDeletion(Reference<T> target, Reference<Identifier> precondition, Identifier requiredValue) {
		traceCurrentState("Before submitConditionalDeletion");
		if (requiredValue.toString().equals(surgeon.valueNode(currentRoot(), precondition).asString())) {
			surgeon.deleteNode(surgeon.nodeInfo(currentRoot(), target));
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
		JsonNode replacement = surgeon.replacementNode(nodeInfo, lastSegment, () -> mapper.convertValue(newValue, JsonNode.class));
		if (nodeInfo.replacementLocation() instanceof Root) {
			switch (contents) {
				case Contents.SingleTree _ -> contents = new Contents.SingleTree(replacement);
				case Contents.MultiTree(var tenantRoots) -> tenantRoots.put(context.getEstablishedTenant(), replacement);
			}
		} else {
			surgeon.replaceNode(nodeInfo, replacement);
		}
	}

	void traceCurrentState(String description) {
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("State {} {}:\n{}", ++updateNumber, description, currentRoot().toPrettyString());
		}
	}

	JsonNode currentRoot() {
		return switch (contents) {
			case Contents.SingleTree(var root) -> root;
			case Contents.MultiTree(var tenantRoots) -> tenantRoots.get(context.getEstablishedTenant());
		};
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(JsonNodeDriver.class);
}
